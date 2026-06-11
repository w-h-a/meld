package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/w-h-a/meld/crdt/lwwregister"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// metaCRDTAddr is the membership Meta key under which a node advertises
// the address of its dedicated CRDT-state UDP port. SWIM gossips Meta
// across the cluster, so every node learns where to send register
// state without a second seed list. The membership port and the CRDT
// port stay decoupled.
const metaCRDTAddr = "crdt_addr"

// palette is the set of "active config" values a node can write. The
// demo models a deploy color. The set is small so two nodes often pick
// from the same handful and concurrent writes are easy to see.
var palette = []string{"blue", "green", "amber", "violet", "crimson"}

// tracer is the demo's instrumentation scope. It is fetched at import
// time, before initTracer installs the real provider. That is safe
// because the otel global delegates. Spans created before the provider
// is set go nowhere, and nothing here traces until main wires it up.
var tracer = otel.Tracer("meld/examples/lwwnode")

func main() {
	nodeID := envOrFail("NODE_ID")
	bindAddr := envOrFail("BIND_ADDR")
	crdtBindAddr := envOrFail("CRDT_BIND_ADDR")
	advertiseAddr := getenv("ADVERTISE_ADDR", nodeID+":7946")
	crdtAdvertiseAddr := getenv("CRDT_ADVERTISE_ADDR", nodeID+":7947")
	otlpEndpoint := getenv("OTLP_ENDPOINT", "jaeger:4317")
	peersRaw := os.Getenv("PEERS")
	probeInterval := durationOr("PROBE_INTERVAL", time.Second)
	probeTimeout := durationOr("PROBE_TIMEOUT", 300*time.Millisecond)
	suspicionMult := intOr("SUSPICION_MULT", 4)
	writeInterval := durationOr("WRITE_INTERVAL", 10*time.Second)
	gossipInterval := durationOr("GOSSIP_INTERVAL", time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown, err := initTracer(context.Background(), nodeID, otlpEndpoint)
	if err != nil {
		log.Fatalf("initTracer: %v", err)
	}
	defer shutdown()

	// SWIM owns the main UDP port for membership and failure detection.
	mg, err := udp.New(gossip.WithBindAddress(bindAddr))
	if err != nil {
		log.Fatalf("membership udp.New: %v", err)
	}

	m, err := swim.New(
		membership.WithGossip(mg),
		membership.WithNodeID(nodeID),
		membership.WithAdvertiseAddress(advertiseAddr),
		membership.WithMeta(map[string]string{metaCRDTAddr: crdtAdvertiseAddr}),
		swim.WithProbeInterval(probeInterval),
		swim.WithProbeTimeout(probeTimeout),
		swim.WithSuspicionMult(suspicionMult),
	)
	if err != nil {
		log.Fatalf("swim.New: %v", err)
	}

	// A second, dedicated UDP transport carries only marshaled register
	// state. The demo owns its Listen and Stop lifecycle.
	cg, err := udp.New(gossip.WithBindAddress(crdtBindAddr))
	if err != nil {
		log.Fatalf("crdt udp.New: %v", err)
	}

	crdtCh, err := cg.Listen(ctx)
	if err != nil {
		log.Fatalf("crdt Listen: %v", err)
	}

	// Join seeded cluster.
	var seeds []string
	if peersRaw != "" {
		seeds = strings.Split(peersRaw, ",")
	}

	if err := m.Join(ctx, seeds); err != nil {
		log.Fatalf("Join: %v", err)
	}
	log.Printf("%s joined; bind=%s advertise=%s crdt=%s seeds=%v", nodeID, bindAddr, advertiseAddr, crdtAdvertiseAddr, seeds)

	state := newRegister()

	go writeLoop(ctx, nodeID, writeInterval, state)
	go receiveLoop(ctx, crdtCh, state, nodeID)
	go broadcastLoop(ctx, cg, m, state, nodeID, gossipInterval)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the three loops, then leave the cluster and close the CRDT
	// transport.
	cancel()

	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaveCancel()
	if err := m.Leave(leaveCtx); err != nil {
		log.Printf("Leave: %v", err)
	}
	if err := cg.Stop(leaveCtx); err != nil {
		log.Printf("crdt Stop: %v", err)
	}
}

// register is the mutable shell around the LWW-Register functional
// core. LWWRegister is an immutable value, so every mutator returns a
// fresh register and a snapshot is safe to marshal while other
// goroutines mutate. The mutex binds the writers (local writes,
// incoming merges) and the readers (broadcast, logging) to one current
// value.
type register struct {
	mu  sync.Mutex
	reg lwwregister.LWWRegister[string]
}

func newRegister() *register {
	return &register{reg: lwwregister.New[string]()}
}

func (r *register) set(nodeID, value string) {
	r.mu.Lock()
	r.reg = r.reg.Set(nodeID, value)
	r.mu.Unlock()
}

func (r *register) merge(other lwwregister.LWWRegister[string]) {
	r.mu.Lock()
	r.reg = r.reg.Merge(other)
	r.mu.Unlock()
}

func (r *register) snapshot() lwwregister.LWWRegister[string] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reg
}

// writeLoop publishes a new "active config" value on a fixed cadence.
// Each write touches only this node's own Tag, because Set takes this
// node's id and only its own. Between writes the cluster has several
// gossip rounds to converge, so a reader watches the value settle and
// then jump when the next write lands.
func writeLoop(ctx context.Context, nodeID string, interval time.Duration, state *register) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			value := palette[rand.IntN(len(palette))]
			state.set(nodeID, value)
		}
	}
}

// receiveLoop drains incoming CRDT datagrams and merges each into local
// state until the context is cancelled or the transport closes the
// channel.
func receiveLoop(ctx context.Context, ch <-chan *gossip.Packet, state *register, nodeID string) {
	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-ch:
			if !ok {
				return
			}
			mergePacket(pkt, state, nodeID)
		}
	}
}

// mergePacket decodes one datagram into a frame, links the merge span
// to the sender's broadcast span through the carried trace context,
// then merges the register the frame holds. Both the frame decode and
// the register unmarshal are datagram-boundary parses, so a failure is
// caller input and gets a bare span event, not a recorded error.
func mergePacket(pkt *gossip.Packet, state *register, nodeID string) {
	var f frame
	if err := json.Unmarshal(pkt.Data, &f); err != nil {
		_, span := tracer.Start(context.Background(), "lwwregister.merge", trace.WithAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.AddEvent("lwwregister.frame_decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	ctx := extractTraceContext(context.Background(), f)

	_, span := tracer.Start(ctx, "lwwregister.merge", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.String("gossip.sender_address", pkt.From.String()),
		attribute.Int("gossip.message_bytes", len(pkt.Data)),
	))
	defer span.End()

	var incoming lwwregister.LWWRegister[string]
	if err := incoming.Unmarshal(f.State, stringDecode); err != nil {
		span.AddEvent("lwwregister.unmarshal_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		return
	}

	before := state.snapshot()
	state.merge(incoming)
	after := state.snapshot()

	winner := writeOutcome(before.Tag(), incoming.Tag())

	span.SetAttributes(
		attribute.String("crdt.value", after.Value()),
		attribute.Int64("crdt.clock", int64(after.Tag().Counter)),
		attribute.String("crdt.writer", after.Tag().Writer),
		attribute.String("crdt.write_winner", winner),
	)

	if winner != "tiebreak" {
		return
	}

	// Concurrent writes. The two Tags share a Lamport counter and
	// differ only in Writer, so neither write saw the other. The Writer
	// tiebreak keeps one value and silently drops the other. We record
	// the dropped value so the LWW tradeoff is visible.
	dropped := before
	if after.Tag() == before.Tag() {
		dropped = incoming
	}

	span.AddEvent("lwwregister.lost_concurrent_write", trace.WithAttributes(
		attribute.String("crdt.kept_value", after.Value()),
		attribute.String("crdt.kept_writer", after.Tag().Writer),
		attribute.String("crdt.dropped_value", dropped.Value()),
		attribute.String("crdt.dropped_writer", dropped.Tag().Writer),
		attribute.Int64("crdt.clock", int64(after.Tag().Counter)),
	))

	log.Printf("%s lost concurrent write: kept=%q(%s) dropped=%q(%s) clock=%d",
		nodeID, after.Value(), after.Tag().Writer, dropped.Value(), dropped.Tag().Writer, after.Tag().Counter)
}

// writeOutcome names which write the register's totally ordered Tag
// keeps when local merges incoming.
//
//   - "remote": incoming has the strictly greater Lamport counter, so
//     it saw at least as many writes as local and wins causally.
//   - "local": local has the strictly greater counter, so incoming is
//     causally stale and is discarded.
//   - "tiebreak": the counters are equal and the writers differ. Equal
//     Lamport counters from two writers are provably concurrent. If
//     either write had seen the other its counter would be strictly
//     greater. So the Writer id breaks the tie and one value is lost.
//
// Equal counter and equal writer is the same write arriving again. The
// merge is idempotent, so the outcome is "local" and nothing changes.
//
// Worked example. Both nodes start from a register at counter 1.
//
//	n1: Set("n1", "blue")   tag=(2, n1)
//	n2: Set("n2", "green")  tag=(2, n2)
//
// Neither saw the other, so the counters tie at 2. Writer "n1" < "n2",
// so n2's "green" wins and n1's "blue" is the lost concurrent write.
//
// Scope note. A scalar Lamport counter can only prove concurrency when
// the counters tie. Two concurrent writes whose counters differ are
// ordered by the counter and not flagged. That is correct LWW
// behavior. The register keeps the higher Tag either way.
func writeOutcome(local, incoming lwwregister.Tag) string {
	switch {
	case local.Counter < incoming.Counter:
		return "remote"
	case local.Counter > incoming.Counter:
		return "local"
	case local.Writer == incoming.Writer:
		return "local"
	default:
		return "tiebreak"
	}
}

// broadcastLoop ships full local state to live peers once per interval.
// Full-state anti-entropy is idempotent, so loss, reordering, and
// duplicates do not corrupt convergence.
func broadcastLoop(ctx context.Context, transport gossip.Gossip, m membership.Membership, state *register, nodeID string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			broadcastState(transport, m, state, nodeID)
		}
	}
}

func broadcastState(transport gossip.Gossip, m membership.Membership, state *register, nodeID string) {
	snap := state.snapshot()

	data, err := snap.Marshal(stringEncode)
	if err != nil {
		_, span := tracer.Start(context.Background(), "lwwregister.broadcast")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return
	}

	peers := crdtPeers(m, transport, nodeID)

	ctx, span := tracer.Start(context.Background(), "lwwregister.broadcast", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.String("crdt.value", snap.Value()),
		attribute.Int64("crdt.clock", int64(snap.Tag().Counter)),
		attribute.String("crdt.writer", snap.Tag().Writer),
		attribute.Int("crdt.peer_count", len(peers)),
	))
	defer span.End()

	log.Printf("%s value=%q clock=%d writer=%s peers=%d", nodeID, snap.Value(), snap.Tag().Counter, snap.Tag().Writer, len(peers))

	if len(peers) == 0 {
		return
	}

	// Wrap the register in a frame that carries this broadcast span's
	// trace context. Every receiver extracts it and starts its merge
	// span as a child, so one write becomes one distributed trace that
	// fans out to each peer. This mirrors the SWIM envelope.
	f := frame{State: data}
	injectTraceContext(ctx, &f)

	payload, err := json.Marshal(f)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	span.SetAttributes(attribute.Int("gossip.message_bytes", len(payload)))

	if err := transport.Broadcast(ctx, peers, payload); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// crdtPeers resolves the CRDT addresses of every live, non-self member
// that has advertised one through membership Meta. A member whose Meta
// has not yet propagated is skipped this round and picked up once SWIM
// converges.
func crdtPeers(m membership.Membership, transport gossip.Transport, selfID string) []net.Addr {
	members := m.Members()
	addrs := make([]net.Addr, 0, len(members))

	for _, n := range members {
		if n.ID == selfID || n.State != membership.Alive {
			continue
		}

		raw := n.Meta[metaCRDTAddr]
		if raw == "" {
			continue
		}

		addr, err := transport.Resolve(raw)
		if err != nil {
			continue
		}

		addrs = append(addrs, addr)
	}

	return addrs
}

// stringEncode and stringDecode are the value codec for a string
// register. The register treats T as opaque, so the demo supplies the
// codec at the marshal boundary. These match the codec the lwwregister
// tests use.
func stringEncode(s string) ([]byte, error) { return []byte(s), nil }
func stringDecode(b []byte) (string, error) { return string(b), nil }

// frame is the on-the-wire form of a gossiped register. State holds the
// marshaled register. Carrier holds the propagated trace context, so a
// receiver starts its merge span as a child of the sender's broadcast
// span and one write becomes one distributed trace. This mirrors the
// SWIM envelope, which carries the same trace context map.
type frame struct {
	Carrier map[string]string `json:"carrier,omitempty"`
	State   []byte            `json:"state"`
}

func injectTraceContext(ctx context.Context, f *frame) {
	if f.Carrier == nil {
		f.Carrier = map[string]string{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(f.Carrier))
}

func extractTraceContext(ctx context.Context, f frame) context.Context {
	if len(f.Carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(f.Carrier))
}

func initTracer(ctx context.Context, serviceName, endpoint string) (func(), error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}, nil
}

func envOrFail(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func durationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func intOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
