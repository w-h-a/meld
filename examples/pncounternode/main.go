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
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt/pncounter"
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
// across the cluster, so every node learns where to send PN-Counter
// state without a second seed list. The membership port and the CRDT
// port stay decoupled.
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
	eventInterval := durationOr("EVENT_INTERVAL", 200*time.Millisecond)
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

	// The Replicator runs Algorithm 1 anti-entropy over the PN-Counter:
	// it owns the gossip-and-merge loop.
	r, err := basic.New(
		antientropy.WithInitial(pncounter.New()),
		antientropy.WithCodec(encodeCounter, decodeCounter),
		antientropy.WithPeerAddress[pncounter.PNCounter](crdtPeerAddress),
		antientropy.WithOnReceive(onReceive(nodeID)),
		antientropy.WithOnSend(onSend(nodeID)),
		antientropy.WithTransport[pncounter.PNCounter](cg),
		antientropy.WithMembership[pncounter.PNCounter](m),
		antientropy.WithInterval[pncounter.PNCounter](gossipInterval),
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

	go eventLoop(ctx, nodeID, eventInterval, decrementProb, r)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s leaving", nodeID)

	// Stop the three loops, then leave the cluster and close the
	// CRDT transport.
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

// encodeCounter and decodeCounter adapt the PN-Counter's own
// marshaling to the antientropy codec the Replicator ships state with.
func encodeCounter(c pncounter.PNCounter) ([]byte, error) {
	return c.Marshal()
}

func decodeCounter(b []byte) (pncounter.PNCounter, error) {
	var c pncounter.PNCounter
	err := c.Unmarshal(b)
	return c, err
}

// crdtPeerAddress resolves a member's CRDT-state address from the
// crdt_addr it advertises through membership Meeta.
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

// onSend enriches the Replicator's gossip span with the local reading
// shipped this round.
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
