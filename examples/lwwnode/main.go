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
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/lwwregister"
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
// across the cluster, so every node learns where to send register
// state without a second seed list. The membership port and the CRDT
// port stay decoupled.
const metaCRDTAddr = "crdt_addr"

// palette is the set of "active config" values a node can write. The
// demo models a deploy color. The set is small so two nodes often pick
// from the same handful and concurrent writes are easy to see.
var palette = []string{"blue", "green", "amber", "violet", "crimson"}

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

	// The Replicator runs Algorithm 1 anti-entropy over the register.
	r, err := basic.New(
		antientropy.WithInitial(lwwregister.New[string]()),
		antientropy.WithCodec(encodeRegister, decodeRegister),
		antientropy.WithPeerAddress[lwwregister.LWWRegister[string]](crdtPeerAddress),
		antientropy.WithOnReceive(onReceive(nodeID)),
		antientropy.WithOnSend(onSend(nodeID)),
		antientropy.WithTransport[lwwregister.LWWRegister[string]](cg),
		antientropy.WithMembership[lwwregister.LWWRegister[string]](m),
		antientropy.WithInterval[lwwregister.LWWRegister[string]](gossipInterval),
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

	go eventLoop(ctx, nodeID, writeInterval, r)

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

// encodeRegister and decodeRegister adapt the register's own marshaling
// to the antientropy codec the Replicator ships state with.
func encodeRegister(r lwwregister.LWWRegister[string]) ([]byte, error) {
	return r.Marshal(crdt.StringEncode)
}

func decodeRegister(b []byte) (lwwregister.LWWRegister[string], error) {
	var r lwwregister.LWWRegister[string]
	err := r.Unmarshal(b, crdt.StringDecode)
	return r, err
}

// crdtPeerAddress resolves a member's CRDT-state address from the
// crdt_addr it advertises through membership Meeta.
func crdtPeerAddress(n membership.Node) (string, bool) {
	addr := n.Meta[metaCRDTAddr]
	return addr, addr != ""
}

// onReceive enriches the Replicator's receive span with the merge
// outcome.
func onReceive(nodeID string) antientropy.OnReceive[lwwregister.LWWRegister[string]] {
	return func(ctx context.Context, before, delta, after lwwregister.LWWRegister[string]) {
		// Name which write the register's totally ordered Tag kept.
		//   - "remote": delta has the strictly greater Lamport counter, so
		//     it saw at least as many writes as local and wins causally.
		//   - "local": local has the strictly greater counter (delta is
		//     causally stale) or the Tags are identical (a duplicate).
		//   - "tiebreak": counters tie and writers differ. Equal counters
		//     from two writers are provably concurrent, since either write
		//     seeing the other would carry a strictly greater counter, so
		//     the Writer id breaks the tie and one value is silently lost.
		local, incoming := before.Tag(), delta.Tag()
		winner := "tiebreak"
		switch {
		case local.Counter < incoming.Counter:
			winner = "remote"
		case local.Counter > incoming.Counter:
			winner = "local"
		case local.Writer == incoming.Writer:
			winner = "local"
		}

		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.String("crdt.value", after.Value()),
			attribute.Int64("crdt.clock", int64(after.Tag().Counter)),
			attribute.String("crdt.writer", after.Tag().Writer),
			attribute.String("crdt.write_winner", winner),
		)

		if winner != "tiebreak" {
			return
		}

		// Concurrent writes tied on the Lamport counter. The Writer
		// tiebreak keeps one value and silently drops the other. Record the
		// dropped value so the loss is visible. after.Tag() == before.Tag()
		// means this node's own value won and the incoming delta was
		// dropped; otherwise the incoming won and this node's value was.
		dropped := before
		if after.Tag() == before.Tag() {
			dropped = delta
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
}

// onSend enriches the Replicator's gossip span with the register value
// shipped this round.
func onSend(nodeID string) antientropy.OnSend[lwwregister.LWWRegister[string]] {
	return func(ctx context.Context, state lwwregister.LWWRegister[string]) {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("crdt.node_id", nodeID),
			attribute.String("crdt.value", state.Value()),
			attribute.Int64("crdt.clock", int64(state.Tag().Counter)),
			attribute.String("crdt.writer", state.Tag().Writer),
		)
	}
}

// eventLoop publishes a new 'active config' value on a fixed cadence.
func eventLoop(
	ctx context.Context,
	nodeID string,
	interval time.Duration,
	r antientropy.Replicator[lwwregister.LWWRegister[string]],
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			value := palette[rand.IntN(len(palette))]
			r.Submit(r.State().Set(nodeID, value))
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
