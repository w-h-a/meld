package main

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/causal"
	"github.com/w-h-a/meld/crdt/pncounter"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/tcp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/phi"
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

// metaCRDTAddr is the membership Meta key under which a node advertises the
// address of its dedicated CRDT-state TCP port. phi carries each node's Meta
// on its own heartbeats, so a node learns a peer's crdt_addr the first time it
// hears from that peer. phi does not disseminate, so PEERS must list every
// other node (a full mesh) for every crdt_addr to reach the whole cluster. The
// membership port and the CRDT port stay decoupled.
const metaCRDTAddr = "crdt_addr"

func main() {
	nodeID := envOrFail("NODE_ID")
	bindAddr := envOrFail("BIND_ADDR")
	crdtBindAddr := envOrFail("CRDT_BIND_ADDR")
	advertiseAddr := getenv("ADVERTISE_ADDR", nodeID+":7946")
	crdtAdvertiseAddr := getenv("CRDT_ADVERTISE_ADDR", nodeID+":7947")
	storePath := getenv("STORE_PATH", nodeID+".db")
	otlpEndpoint := getenv("OTLP_ENDPOINT", "jaeger:4317")
	peersRaw := os.Getenv("PEERS")
	heartbeatInterval := durationOr("HEARTBEAT_INTERVAL", time.Second)
	phiHigh := floatOr("PHI_HIGH_THRESHOLD", 8.0)
	phiLow := floatOr("PHI_LOW_THRESHOLD", 1.0)
	windowSize := intOr("WINDOW_SIZE", 1000)
	minStdDev := durationOr("MIN_STDDEV", 100*time.Millisecond)
	suspectDwell := durationOr("SUSPECT_DWELL", 2*time.Second)
	reapDwell := durationOr("REAP_DWELL", 30*time.Second)
	eventInterval := durationOr("EVENT_INTERVAL", 200*time.Millisecond)
	decrementProb := floatOr("DECREMENT_PROBABILITY", 0.5)
	gossipInterval := durationOr("GOSSIP_INTERVAL", time.Second)
	gcInterval := durationOr("GC_INTERVAL", 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown, err := initTracer(context.Background(), nodeID, otlpEndpoint)
	if err != nil {
		log.Fatalf("initTracer: %v", err)
	}
	defer shutdown()

	// phi owns the main TCP port for membership and failure detection.
	mg, err := tcp.New(gossip.WithBindAddress(bindAddr))
	if err != nil {
		log.Fatalf("membership tcp.New: %v", err)
	}

	m, err := phi.New(
		membership.WithGossip(mg),
		membership.WithNodeID(nodeID),
		membership.WithAdvertiseAddress(advertiseAddr),
		membership.WithMeta(map[string]string{metaCRDTAddr: crdtAdvertiseAddr}),
		phi.WithHeartbeatInterval(heartbeatInterval),
		phi.WithPhiHighThreshold(phiHigh),
		phi.WithPhiLowThreshold(phiLow),
		phi.WithWindowSize(windowSize),
		phi.WithMinStdDev(minStdDev),
		phi.WithSuspectDwell(suspectDwell),
		phi.WithReapDwell(reapDwell),
	)
	if err != nil {
		log.Fatalf("phi.New: %v", err)
	}

	// A second, dedicated TCP transport carries only marshaled PN-Counter
	// state.
	cg, err := tcp.New(gossip.WithBindAddress(crdtBindAddr))
	if err != nil {
		log.Fatalf("crdt tcp.New: %v", err)
	}

	// causal: durable store. The causal node persists (seq, state) on every
	// transition and reloads it on restart.
	st, err := sqlite.New(store.WithLocation(storePath))
	if err != nil {
		log.Fatalf("sqlite.New: %v", err)
	}

	// The Replicator runs Algorithm 2 (causal) anti-entropy over the
	// PN-Counter.
	r, err := causal.New(
		antientropy.WithInitial(pncounter.New()),
		antientropy.WithCodec(encodeCounter, decodeCounter),
		antientropy.WithPeerAddress[pncounter.PNCounter](crdtPeerAddress),
		antientropy.WithOnReceive(onReceive(nodeID)),
		antientropy.WithOnSend(onSend(nodeID)),
		antientropy.WithTransport[pncounter.PNCounter](cg),
		antientropy.WithMembership[pncounter.PNCounter](m),
		antientropy.WithStore[pncounter.PNCounter](st),
		antientropy.WithInterval[pncounter.PNCounter](gossipInterval),
		causal.WithGCInterval[pncounter.PNCounter](gcInterval),
	)
	if err != nil {
		log.Fatalf("causal.New: %v", err)
	}

	// Start opens the CRDT transport's listener and the gossip loops.
	if err := r.Start(ctx); err != nil {
		log.Fatalf("replicator Start: %v", err)
	}

	// Join seeded cluster. phi does not disseminate membership, so PEERS must
	// list every other node (a full mesh), not just one bootstrap.
	var seeds []string
	if peersRaw != "" {
		seeds = strings.Split(peersRaw, ",")
	}

	if err := m.Join(ctx, seeds); err != nil {
		log.Fatalf("Join: %v", err)
	}
	log.Printf("%s joined; bind=%s advertise=%s crdt=%s store=%s seeds=%v", nodeID, bindAddr, advertiseAddr, crdtAdvertiseAddr, storePath, seeds)

	// Watch logs membership events (Join, Update, Fail, Leave) so a kill or a
	// restart shows up in the logs, not only in Jaeger.
	watch, err := m.Watch()
	if err != nil {
		log.Fatalf("Watch: %v", err)
	}
	go func() {
		for ev := range watch {
			log.Printf("event: type=%d node=%s state=%d", ev.Type, ev.Node.ID, ev.Node.State)
		}
	}()

	go eventLoop(ctx, nodeID, eventInterval, decrementProb, r)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the loop, then leave the cluster, then close the CRDT transport,
	// and, finally, close the store.
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

// encodeCounter and decodeCounter adapt the PN-Counter's own marshaling to the
// antientropy codec the Replicator ships state with.
func encodeCounter(c pncounter.PNCounter) ([]byte, error) {
	return c.Marshal()
}

func decodeCounter(b []byte) (pncounter.PNCounter, error) {
	var c pncounter.PNCounter
	err := c.Unmarshal(b)
	return c, err
}

// crdtPeerAddress resolves a member's CRDT-state address from the crdt_addr it
// advertises through membership Meta.
func crdtPeerAddress(n membership.Node) (string, bool) {
	addr := n.Meta[metaCRDTAddr]
	return addr, addr != ""
}

// onReceive enriches the Replicator's receive span with the converged
// PN-Counter reading after each merge.
func onReceive(nodeID string) antientropy.OnReceive[pncounter.PNCounter] {
	return func(ctx context.Context, before, _, after pncounter.PNCounter) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int64("crdt.value", after.Value()),
			attribute.Int64("crdt.p_sum", int64(after.Increments())),
			attribute.Int64("crdt.n_sum", int64(after.Decrements())),
		)
	}
}

// onSend enriches the Replicator's gossip span with the local reading shipped
// this round.
func onSend(nodeID string) antientropy.OnSend[pncounter.PNCounter] {
	return func(ctx context.Context, state pncounter.PNCounter) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.Int64("crdt.value", state.Value()),
			attribute.Int64("crdt.p_sum", int64(state.Increments())),
			attribute.Int64("crdt.n_sum", int64(state.Decrements())),
		)
		log.Printf("%s value=%d p=%d n=%d", nodeID, state.Value(), state.Increments(), state.Decrements())
	}
}

// eventLoop simulates a fluctuating pool of open conns.
func eventLoop(
	ctx context.Context,
	nodeID string,
	interval time.Duration,
	decrementProb float64,
	r antientropy.Replicator[pncounter.PNCounter],
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if rand.Float64() < decrementProb {
				r.Submit(r.State().DecrementDelta(nodeID))
				continue
			}
			r.Submit(r.State().IncrementDelta(nodeID))
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
