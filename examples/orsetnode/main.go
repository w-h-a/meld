package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
	"github.com/w-h-a/meld/util/tracecontext"
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
// across the cluster, so every node learns where to send set state
// without a second seed list. The membership port and the CRDT port
// stay decoupled.
const metaCRDTAddr = "crdt_addr"

// tracer is the demo's instrumentation scope. It is fetched at import
// time, before initTracer installs the real provider. That is safe
// because the otel global delegates. Spans created before the provider
// is set go nowhere, and nothing here traces until main wires it up.
var tracer = otel.Tracer("meld/examples/orsetnode")

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
	eventsPerSec := intOr("EVENTS_PER_SEC", 1)
	removeProb := floatOr("REMOVE_PROBABILITY", 0.3)
	poolSize := intOr("WORKLOAD_POOL_SIZE", 8)
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

	// A second, dedicated UDP transport carries only marshaled set
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

	state := newSet()

	go eventLoop(ctx, nodeID, eventsPerSec, removeProb, poolSize, state)
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

// set is the mutable shell around the OR-Set functional core. ORSet is
// an immutable value, so every mutator returns a fresh set and a
// snapshot is safe to marshal while other goroutines mutate. The mutex
// binds the writers (local events, incoming merges) and the readers
// (broadcast, logging) to one current state. lastAdded and lastRemoved
// hold this node's most recent local operation. removedHere holds
// elements this node removed and has not locally re-added, so a merge
// can report when a peer's add brings one back.
type set struct {
	mu          sync.Mutex
	reg         orset.ORSet[string]
	removedHere map[string]struct{}
	lastAdded   string
	lastRemoved string
}

func newSet() *set {
	return &set{reg: orset.New[string](), removedHere: map[string]struct{}{}}
}

func (s *set) add(nodeID, element string) {
	s.mu.Lock()
	s.reg = s.reg.Add(nodeID, element)
	delete(s.removedHere, element)
	s.lastAdded = element
	s.mu.Unlock()
}

func (s *set) remove(element string) {
	s.mu.Lock()
	s.reg = s.reg.Remove(element)
	s.removedHere[element] = struct{}{}
	s.lastRemoved = element
	s.mu.Unlock()
}

// merge applies other and reports the elements this node had removed
// locally that the merge brought back. A resurrected element is the
// add-wins policy in action: a peer's add carried a tag this node's
// remove never observed, so the add survives the remove.
func (s *set) merge(other orset.ORSet[string]) (resurrected []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reg = s.reg.Merge(other)

	for e := range s.removedHere {
		if s.reg.Contains(e) {
			resurrected = append(resurrected, e)
			delete(s.removedHere, e)
		}
	}

	sort.Strings(resurrected)
	return resurrected
}

func (s *set) snapshot() orset.ORSet[string] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reg
}

func (s *set) lastOps() (added, removed string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAdded, s.lastRemoved
}

// eventLoop simulates a fluctuating set of scheduled workloads. Each
// tick it either submits a workload (Add) or cancels one (Remove),
// drawing the id from a fixed pool. A small pool makes two nodes touch
// the same id concurrently, which is what exercises the add-wins
// policy. Add touches only this node's own slot in the version vector,
// because Add takes this node's id and only its own.
func eventLoop(ctx context.Context, nodeID string, eventsPerSec int, removeProb float64, poolSize int, state *set) {
	if eventsPerSec < 1 {
		eventsPerSec = 1
	}
	if poolSize < 1 {
		poolSize = 1
	}

	ticker := time.NewTicker(time.Second / time.Duration(eventsPerSec))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id := fmt.Sprintf("workload-%d", rand.IntN(poolSize))
			if rand.Float64() < removeProb {
				state.remove(id)
				continue
			}
			state.add(nodeID, id)
		}
	}
}

// receiveLoop drains incoming CRDT datagrams and merges each into local
// state until the context is cancelled or the transport closes the
// channel.
func receiveLoop(ctx context.Context, ch <-chan *gossip.Packet, state *set, nodeID string) {
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
// then merges the set the frame holds. Both the frame decode and the
// set unmarshal are datagram-boundary parses, so a failure is caller
// input and gets a bare span event, not a recorded error.
func mergePacket(pkt *gossip.Packet, state *set, nodeID string) {
	var f frame
	if err := json.Unmarshal(pkt.Data, &f); err != nil {
		_, span := tracer.Start(context.Background(), "orset.merge", trace.WithAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.AddEvent("orset.frame_decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	ctx := tracecontext.Extract(context.Background(), f.Carrier)

	_, span := tracer.Start(ctx, "orset.merge", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.String("gossip.sender_address", pkt.From.String()),
		attribute.Int("gossip.message_bytes", len(pkt.Data)),
	))
	defer span.End()

	var incoming orset.ORSet[string]
	if err := incoming.Unmarshal(f.State, crdt.StringDecode); err != nil {
		span.AddEvent("orset.unmarshal_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		return
	}

	beforeElements := state.snapshot().Elements()
	resurrected := state.merge(incoming)
	after := state.snapshot()
	afterElements := after.Elements()

	added, removed := elementDelta(beforeElements, afterElements)
	changed := len(added) > 0 || len(removed) > 0

	// Put the outcome on tags, not an event, so it shows in the span's
	// tag list and is filterable. crdt.changed filters out the no-op
	// merges that dominate full-state gossip. crdt.add_wins flags the
	// headline: a peer's add resurrected an element this node removed.
	span.SetAttributes(
		attribute.Int("crdt.element_count", len(afterElements)),
		attribute.Int("crdt.live_triple_count", after.LiveCount()),
		attribute.Bool("crdt.changed", changed),
	)

	if len(added) > 0 {
		span.SetAttributes(attribute.String("crdt.elements_added", strings.Join(added, ",")))
	}
	if len(removed) > 0 {
		span.SetAttributes(attribute.String("crdt.elements_removed", strings.Join(removed, ",")))
	}
	if len(resurrected) > 0 {
		span.SetAttributes(
			attribute.Bool("crdt.add_wins", true),
			attribute.String("crdt.add_wins_elements", strings.Join(resurrected, ",")),
		)
	}

	if !changed {
		return
	}

	log.Printf("%s merged from %s: added=[%s] removed=[%s] add_wins=[%s] elements=%d triples=%d",
		nodeID, pkt.From.String(), strings.Join(added, ","), strings.Join(removed, ","),
		strings.Join(resurrected, ","), len(afterElements), after.LiveCount())
}

// elementDelta returns the elements present in after but not before
// (gained on the merge) and present in before but not after (lost on
// the merge), each sorted.
func elementDelta(before, after []string) (added, removed []string) {
	beforeSet := make(map[string]struct{}, len(before))
	for _, e := range before {
		beforeSet[e] = struct{}{}
	}

	afterSet := make(map[string]struct{}, len(after))
	for _, e := range after {
		afterSet[e] = struct{}{}
	}

	for e := range afterSet {
		if _, ok := beforeSet[e]; !ok {
			added = append(added, e)
		}
	}

	for e := range beforeSet {
		if _, ok := afterSet[e]; !ok {
			removed = append(removed, e)
		}
	}

	sort.Strings(added)
	sort.Strings(removed)

	return added, removed
}

// broadcastLoop ships full local state to live peers once per interval.
// Full-state anti-entropy is idempotent, so loss, reordering, and
// duplicates do not corrupt convergence.
func broadcastLoop(ctx context.Context, transport gossip.Gossip, m membership.Membership, state *set, nodeID string, interval time.Duration) {
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

func broadcastState(transport gossip.Gossip, m membership.Membership, state *set, nodeID string) {
	snap := state.snapshot()
	lastAdded, lastRemoved := state.lastOps()

	data, err := snap.Marshal(crdt.StringEncode)
	if err != nil {
		_, span := tracer.Start(context.Background(), "orset.broadcast")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return
	}

	elements := snap.Elements()
	peers := crdtPeers(m, transport, nodeID)

	ctx, span := tracer.Start(context.Background(), "orset.broadcast", trace.WithAttributes(
		attribute.String("crdt.node_id", nodeID),
		attribute.Int("crdt.element_count", len(elements)),
		attribute.Int("crdt.live_triple_count", snap.LiveCount()),
		attribute.String("crdt.last_added", lastAdded),
		attribute.String("crdt.last_removed", lastRemoved),
		attribute.Int("crdt.peer_count", len(peers)),
	))
	defer span.End()

	log.Printf("%s elements=%d triples=%d last_added=%q last_removed=%q peers=%d",
		nodeID, len(elements), snap.LiveCount(), lastAdded, lastRemoved, len(peers))

	if len(peers) == 0 {
		return
	}

	// Wrap the set in a frame that carries this broadcast span's trace
	// context. Every receiver extracts it and starts its merge span as a
	// child, so one broadcast becomes one distributed trace that fans
	// out to each peer. This mirrors the SWIM envelope.
	f := frame{State: data}
	f.Carrier = tracecontext.Inject(ctx)

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

// frame is the on-the-wire form of a gossiped set. State holds the
// marshaled set. Carrier holds the propagated trace context, so a
// receiver starts its merge span as a child of the sender's broadcast
// span and one write becomes one distributed trace. This mirrors the
// SWIM envelope, which carries the same trace context map.
type frame struct {
	Carrier map[string]string `json:"carrier,omitempty"`
	State   []byte            `json:"state"`
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
