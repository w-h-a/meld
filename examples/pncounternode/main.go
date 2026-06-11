package main

import (
	"context"
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

	"github.com/w-h-a/meld/crdt/pncounter"
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
// across the cluster, so every node learns where to send PN-Counter
// state without a second seed list. The membership port and the CRDT
// port stay decoupled.
const metaCRDTAddr = "crdt_addr"

// tracer is the demo's instrumentation scope. It is fetched at import
// time, before initTracer installs the real provider. That is safe
// because the otel global delegates. Spans created before the provider
// is set go nowhere, and nothing here traces until main wires it up.
var tracer = otel.Tracer("meld/examples/pncounternode")

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
	eventsPerSec := intOr("EVENTS_PER_SEC", 5)
	decrementProb := floatOr("DECREMENT_PROBABILITY", 0.5)
	gossipInterval := durationOr("GOSSIP_INTERVAL", 2*time.Second)

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

	// A second, dedicated UDP transport carries only marshaled
	// PN-Counter state. The demo owns its Listen and Stop lifecycle.
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

	state := newCounter()

	go eventLoop(ctx, nodeID, eventsPerSec, decrementProb, state)
	go receiveLoop(ctx, crdtCh, state, nodeID)
	go broadcastLoop(ctx, cg, m, state, nodeID, gossipInterval)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the three loops, then leave the cluster and close the
	// CRDT transport.
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

// counter is the mutable shell around the PN-Counter functional core.
// PN-Counter is an immutable value, so every mutator returns a fresh
// counter and a snapshot is safe to marshal while other goroutines
// mutate. The mutex binds the three writers (local events, incoming
// merges) and the readers (broadcast, logging) to one current value.
type counter struct {
	mu sync.Mutex
	pn pncounter.PNCounter
}

func newCounter() *counter {
	return &counter{pn: pncounter.New()}
}

func (c *counter) increment(id string) {
	c.mu.Lock()
	c.pn = c.pn.Increment(id)
	c.mu.Unlock()
}

func (c *counter) decrement(id string) {
	c.mu.Lock()
	c.pn = c.pn.Decrement(id)
	c.mu.Unlock()
}

func (c *counter) merge(other pncounter.PNCounter) {
	c.mu.Lock()
	c.pn = c.pn.Merge(other)
	c.mu.Unlock()
}

func (c *counter) snapshot() pncounter.PNCounter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pn
}

// eventLoop simulates a fluctuating pool of open connections. Each tick
// it either opens a connection (increment) or closes one (decrement),
// touching only this node's own slot. When DECREMENT_PROBABILITY exceeds
// 0.5 across the cluster, decrements outpace increments and the
// converged reading trends negative.
func eventLoop(ctx context.Context, nodeID string, eventsPerSec int, decrementProb float64, state *counter) {
	if eventsPerSec < 1 {
		eventsPerSec = 1
	}

	ticker := time.NewTicker(time.Second / time.Duration(eventsPerSec))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if rand.Float64() < decrementProb {
				state.decrement(nodeID)
				continue
			}
			state.increment(nodeID)
		}
	}
}

// receiveLoop drains incoming CRDT datagrams and merges each into local
// state until the context is cancelled or the transport closes the
// channel.
func receiveLoop(ctx context.Context, ch <-chan *gossip.Packet, state *counter, nodeID string) {
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

// mergePacket parses one datagram into a PN-Counter and merges it. The
// datagram boundary is where untrusted bytes become a domain value, so
// a parse failure is caller input and gets a bare span event, not a
// recorded error.
func mergePacket(pkt *gossip.Packet, state *counter, nodeID string) {
	_, span := tracer.Start(context.Background(), "pncounter.merge", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.String("gossip.sender_address", pkt.From.String()),
		attribute.Int("gossip.message_bytes", len(pkt.Data)),
	))
	defer span.End()

	var incoming pncounter.PNCounter
	if err := incoming.Unmarshal(pkt.Data); err != nil {
		span.AddEvent("pncounter.unmarshal_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		return
	}

	state.merge(incoming)

	snap := state.snapshot()
	span.SetAttributes(
		attribute.Int64("crdt.value", snap.Value()),
		attribute.Int64("crdt.p_sum", int64(snap.Increments())),
		attribute.Int64("crdt.n_sum", int64(snap.Decrements())),
	)
}

// broadcastLoop ships full local state to live peers once per interval.
// Full-state anti-entropy is idempotent, so loss, reordering, and
// duplicates do not corrupt convergence.
func broadcastLoop(ctx context.Context, transport gossip.Gossip, m membership.Membership, state *counter, nodeID string, interval time.Duration) {
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

func broadcastState(transport gossip.Gossip, m membership.Membership, state *counter, nodeID string) {
	snap := state.snapshot()

	data, err := snap.Marshal()
	if err != nil {
		_, span := tracer.Start(context.Background(), "pncounter.broadcast")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return
	}

	peers := crdtPeers(m, transport, nodeID)

	ctx, span := tracer.Start(context.Background(), "pncounter.broadcast", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.Int64("crdt.value", snap.Value()),
		attribute.Int64("crdt.p_sum", int64(snap.Increments())),
		attribute.Int64("crdt.n_sum", int64(snap.Decrements())),
		attribute.Int("crdt.peer_count", len(peers)),
		attribute.Int("gossip.message_bytes", len(data)),
	))
	defer span.End()

	log.Printf("%s value=%d p=%d n=%d peers=%d", nodeID, snap.Value(), snap.Increments(), snap.Decrements(),
		len(peers))

	if len(peers) == 0 {
		return
	}

	if err := transport.Broadcast(ctx, peers, data); err != nil {
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

func floatOr(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
