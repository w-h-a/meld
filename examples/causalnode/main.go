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
	"syscall"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/causal"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
	"github.com/w-h-a/meld/store"
	"github.com/w-h-a/meld/store/sqlite"
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
	storePath := getenv("STORE_PATH", nodeID+".db") // causal: durable slot
	otlpEndpoint := getenv("OTLP_ENDPOINT", "jaeger:4317")
	peersRaw := os.Getenv("PEERS")
	probeInterval := durationOr("PROBE_INTERVAL", time.Second)
	probeTimeout := durationOr("PROBE_TIMEOUT", 300*time.Millisecond)
	suspicionMult := intOr("SUSPICION_MULT", 4)
	eventsInterval := durationOr("EVENTS_INTERVAL", time.Second)
	removeProb := floatOr("REMOVE_PROBABILITY", 0.3)
	poolSize := intOr("WORKLOAD_POOL_SIZE", 8)
	gossipInterval := durationOr("GOSSIP_INTERVAL", time.Second)
	gcInterval := durationOr("GC_INTERVAL", 10*time.Second)

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

	// A second, dedicated UDP transport carries only marshaled set state.
	cg, err := udp.New(gossip.WithBindAddress(crdtBindAddr))
	if err != nil {
		log.Fatalf("crdt udp.New: %v", err)
	}

	// causal: durable store. The causal node persists (seq, state) on every
	// transition and reloads it on restart.
	st, err := sqlite.New(store.WithLocation(storePath))
	if err != nil {
		log.Fatalf("sqlite.New: %v", err)
	}

	// The Replicator runs Algorithm 2 (causal) anti-entropy over the ORSet.
	r, err := causal.New(
		antientropy.WithInitial(orset.New[string]()),
		antientropy.WithCodec(encodeSet, decodeSet),
		antientropy.WithPeerAddress[orset.ORSet[string]](crdtPeerAddress),
		antientropy.WithOnReceive(onReceive(nodeID)),
		antientropy.WithOnSend(onSend(nodeID)),
		antientropy.WithTransport[orset.ORSet[string]](cg),
		antientropy.WithMembership[orset.ORSet[string]](m),
		antientropy.WithStore[orset.ORSet[string]](st),
		antientropy.WithInterval[orset.ORSet[string]](gossipInterval),
		causal.WithGCInterval[orset.ORSet[string]](gcInterval),
	)
	if err != nil {
		log.Fatalf("causal.New: %v", err)
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
	log.Printf("%s joined; bind=%s advertise=%s crdt=%s store=%s seeds=%v", nodeID, bindAddr, advertiseAddr, crdtAdvertiseAddr, storePath, seeds)

	go eventLoop(ctx, nodeID, eventsInterval, removeProb, poolSize, r)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the loop, then leave the cluster, then close the CRDT
	// transport, and, finally, close the store.
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
	if err := st.Close(leaveCtx); err != nil {
		log.Printf("store Close: %v", err)
	}
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

// onReceive enriches the receive span with what the merge changed and the
// converged size.
func onReceive(nodeID string) antientropy.OnReceive[orset.ORSet[string]] {
	return func(ctx context.Context, before, _, after orset.ORSet[string]) {
		added, removed := elementDelta(before.Elements(), after.Elements())
		changed := len(added) > 0 || len(removed) > 0

		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int("crdt.element_count", len(after.Elements())),
			attribute.Int("crdt.live_triple_count", after.LiveCount()),
			attribute.Bool("crdt.elements_changed", changed),
		)

		if len(added) > 0 {
			span.SetAttributes(attribute.String("crdt.elements_added", strings.Join(added, ",")))
		}
		if len(removed) > 0 {
			span.SetAttributes(attribute.String("crdt.elements_removed", strings.Join(removed, ",")))
		}

		if !changed {
			return
		}

		log.Printf("%s merged: added=[%s] removed=[%s] elements=%d",
			nodeID, strings.Join(added, ","), strings.Join(removed, ","), len(after.Elements()))
	}
}

// elementDelta returns the elements gained and lost between two snapshots.
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

// onSend enriches the gossip span with the set this node is shipping.
func onSend(nodeID string) antientropy.OnSend[orset.ORSet[string]] {
	return func(ctx context.Context, state orset.ORSet[string]) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int("crdt.element_count", len(state.Elements())),
			attribute.Int("crdt.live_triple_count", state.LiveCount()),
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
				continue
			}
			r.Submit(r.State().AddDelta(nodeID, id))
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
