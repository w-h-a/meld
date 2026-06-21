package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	eventsInterval := durationOr("EVENTS_INTERVAL", time.Second)
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

	// intent is the only local state the demo keeps: this node's own
	// remove history, so the receive hook can flag add-wins resurrections.
	in := newIntent()

	// The Replicator runs Algorithm 1 anti-entropy over the ORSet.
	r, err := basic.New(
		antientropy.WithInitial(orset.New[string]()),
		antientropy.WithCodec(encodeSet, decodeSet),
		antientropy.WithPeerAddress[orset.ORSet[string]](crdtPeerAddress),
		antientropy.WithOnReceive(onReceive(nodeID, in)),
		antientropy.WithOnSend(onSend(nodeID, in)),
		antientropy.WithTransport[orset.ORSet[string]](cg),
		antientropy.WithMembership[orset.ORSet[string]](m),
		antientropy.WithInterval[orset.ORSet[string]](gossipInterval),
	)
	if err != nil {
		log.Fatalf("basic.New: %v", err)
	}

	// Start opens the CRDT transport's listener and the gossip loops.
	if err := r.Start(ctx); err != nil {
		log.Fatalf("replicator Start: %v", err)
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

	go eventLoop(ctx, nodeID, eventsInterval, removeProb, poolSize, r, in)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the three loops, then leave the cluster and close the CRDT
	// transport.
	cancel()

	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaveCancel()
	if err := r.Stop(leaveCtx); err != nil {
		log.Printf("replicator Stop: %v", err)
	}
	if err := m.Leave(leaveCtx); err != nil {
		log.Printf("Leave: %v", err)
	}
	if err := cg.Stop(leaveCtx); err != nil {
		log.Printf("crdt Stop: %v", err)
	}
}

// intent tracks this node's own operation history, the app-level state
// the Replicator does not hold. removedHere is the set of elements this
// node removed and has not locally re-added. The receive hook checks it
// against the converged set to flag an add-wins resurrection: a peer's
// concurrent add carried a tag this node's remove never observed, so the
// add survived and the element came back. lastAdded and lastRemoved are
// this node's most recent local ops, surfaced on the gossip span.
type intent struct {
	mu          sync.Mutex
	removedHere map[string]struct{}
	lastAdded   string
	lastRemoved string
}

func newIntent() *intent {
	return &intent{removedHere: map[string]struct{}{}}
}

// added records a local Add: the element is intended live again, so it
// leaves the remove set.
func (in *intent) added(element string) {
	in.mu.Lock()
	delete(in.removedHere, element)
	in.lastAdded = element
	in.mu.Unlock()
}

// removed records a local Remove: the element joins the remove set, where
// a later resurrection check can find it.
func (in *intent) removed(element string) {
	in.mu.Lock()
	in.removedHere[element] = struct{}{}
	in.lastRemoved = element
	in.mu.Unlock()
}

// resurrected returns the elements this node removed that are live again
// in the converged set, clearing each from the remove set as it reports
// it. A resurrected element is add-wins in action.
func (in *intent) resurrected(live orset.ORSet[string]) []string {
	in.mu.Lock()
	defer in.mu.Unlock()

	var out []string
	for e := range in.removedHere {
		if live.Contains(e) {
			out = append(out, e)
			delete(in.removedHere, e)
		}
	}

	sort.Strings(out)
	return out
}

// lastOps returns this node's most recent local add and remove.
func (in *intent) lastOps() (added, removed string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.lastAdded, in.lastRemoved
}

// encodeSet and decodeSet adapt the OR-Set's own marshaling to the
// antientropy codec the Replicator ships state with.
func encodeSet(s orset.ORSet[string]) ([]byte, error) {
	return s.Marshal(crdt.StringEncode)
}

func decodeSet(b []byte) (orset.ORSet[string], error) {
	var s orset.ORSet[string]
	err := s.Unmarshal(b, crdt.StringDecode)
	return s, err
}

// crdtPeerAddress resolves a member's CRDT-state address from the
// crdt_addr it advertises through membership Meeta.
func crdtPeerAddress(n membership.Node) (string, bool) {
	addr := n.Meta[metaCRDTAddr]
	return addr, addr != ""
}

// onReceive enriches the Replicator's receive span with what the merge
// changed. It reports the elements gained and lost, and the headline
// add-wins resurrections: elements this node removed that a peer's
// concurrent add brought back. delta is unused because a resurrection is
// found by checking this node's remove intent against the converged set,
// not by inspecting the incoming fragment.
func onReceive(nodeID string, in *intent) antientropy.OnReceive[orset.ORSet[string]] {
	return func(ctx context.Context, before, _, after orset.ORSet[string]) {
		added, removed := elementDelta(before.Elements(), after.Elements())
		resurrected := in.resurrected(after)
		changed := len(added) > 0 || len(removed) > 0

		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int("crdt.element_count", len(after.Elements())),
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

		log.Printf("%s merged: added=[%s] removed=[%s] add_wins=[%s] elements=%d triples=%d",
			nodeID, strings.Join(added, ","), strings.Join(removed, ","),
			strings.Join(resurrected, ","), len(after.Elements()), after.LiveCount())
	}
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

// onSend enriches the Replicator's gossip span with the set this node is
// shipping and its most recent local ops.
func onSend(nodeID string, in *intent) antientropy.OnSend[orset.ORSet[string]] {
	return func(ctx context.Context, state orset.ORSet[string]) {
		lastAdded, lastRemoved := in.lastOps()
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int("crdt.element_count", len(state.Elements())),
			attribute.Int("crdt.live_triple_count", state.LiveCount()),
			attribute.String("crdt.last_added", lastAdded),
			attribute.String("crdt.last_removed", lastRemoved),
		)
	}
}

// eventLoop simulates a fluctuating set of scheduled workloads.
func eventLoop(
	ctx context.Context,
	nodeID string,
	interval time.Duration,
	removeProb float64,
	poolSize int,
	r antientropy.Replicator[orset.ORSet[string]],
	in *intent,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id := fmt.Sprintf("workload-%d", rand.IntN(poolSize))
			if rand.Float64() < removeProb {
				r.Submit(r.State().RemoveDelta(id))
				in.removed(id)
				continue
			}
			r.Submit(r.State().AddDelta(nodeID, id))
			in.added(id)
		}
	}
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
