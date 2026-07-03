package causal

import (
	"context"
	"errors"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/util/tracecontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// causalReplicator is the causal anti-entropy replicator for a CRDT. It
// ships only the contiguous run of deltas that neighbors have not yet
// acknowledged.
//
// state and seq are durable. Suppose the node has produced and shipped
// neighbor j its deltas 0 through 4, so seq is 5 and j has acked through 5.
// The node crashes. If seq restarts at 0, the next local writes reuse
// seq 0, 1, 2, etc. A late ack for the pre-crash deltas then records j
// as caught up through 5, so the node believes j already holds everything
// below 5 and never ships new deltas to j.
//
// deltas and acks are rebuildable. A restart starts them empty. With no
// acks recorded, the node assumes every neighbor is behind, and with no
// buffered deltas, it ships full state. Neighbors converge and re-ack,
// which refills acks and lets the node resume sending incremental deltas.
// Losing deltas or acks therefore costs only some redundant full-state
// sends, never correctness.
type causalReplicator[T crdt.Equatable[T]] struct {
	options    antientropy.Options[T]
	gcInterval time.Duration

	mtx    sync.RWMutex
	state  T                 // durable
	seq    uint64            // durable
	deltas map[uint64]T      // volatile: seq -> delta
	acks   map[string]uint64 // volatile: neighbor id -> highest acked seq
	round  uint64            // volatile: round-robin cursor over sorted neighbors

	cancel context.CancelFunc
	wg     sync.WaitGroup

	tracer trace.Tracer
}

func New[T crdt.Equatable[T]](opts ...antientropy.Option[T]) (antientropy.Replicator[T], error) {
	options := antientropy.NewOptions(opts...)

	switch {
	case options.Encoder == nil || options.Decoder == nil:
		return nil, errors.New("causal: a codec is required")
	case options.Transport == nil:
		return nil, errors.New("causal: a transport is required")
	case options.Membership == nil:
		return nil, errors.New("causal: a membership source is required")
	case options.Store == nil:
		return nil, errors.New("causal: a store is required")
	}

	r := &causalReplicator[T]{
		options:    options,
		gcInterval: gcIntervalFrom(options.Context),
		mtx:        sync.RWMutex{},
		state:      options.Initial,
		deltas:     map[uint64]T{},
		acks:       map[string]uint64{},
		tracer:     otel.Tracer("meld/antientropy/causal"),
	}

	return r, nil
}

// Submit joins a locally produced delta into the state, buffers it under
// the current sequence, advances the sequence, and persists the new
// durable (seq, state) before returning.
func (r *causalReplicator[T]) Submit(delta T) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.state = r.state.Merge(delta)
	r.deltas[r.seq] = delta
	r.seq++
	r.persist(context.Background())
}

// State returns the current converged CRDT state.
func (r *causalReplicator[T]) State() T {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return r.state
}

// Start loads durable state, opens the transport listener, and launches
// the receive, send, and garbage-collection loops.
func (r *causalReplicator[T]) Start(ctx context.Context) error {
	if err := r.load(ctx); err != nil {
		return err
	}

	loopCtx, cancel := context.WithCancel(ctx)

	ch, err := r.options.Transport.Listen(loopCtx)
	if err != nil {
		cancel()
		return err
	}

	r.cancel = cancel
	r.wg.Add(3)

	go func() {
		defer r.wg.Done()
		r.receiveLoop(loopCtx, ch)
	}()

	go func() {
		defer r.wg.Done()
		r.sendLoop(loopCtx)
	}()

	go func() {
		defer r.wg.Done()
		r.gcLoop(loopCtx)
	}()

	return nil
}

// Stop cancels the loops and waits for them to exit, or returns the
// context's error if they do not exit in time.
func (r *causalReplicator[T]) Stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// load restores the durable (seq, state) blob from the store.
func (r *causalReplicator[T]) load(ctx context.Context) error {
	data, found, err := r.options.Store.Load(ctx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	snap, err := decodeSnapshot(data)
	if err != nil {
		return err
	}

	state, err := r.options.Decoder(snap.State)
	if err != nil {
		return err
	}

	r.state = state
	r.seq = snap.Seq

	return nil
}

func (r *causalReplicator[T]) receiveLoop(ctx context.Context, ch <-chan *gossip.Packet) {
	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-ch:
			if !ok {
				return
			}
			r.receive(pkt)
		}
	}
}

// receive decodes one gossip.Packet and dispatches on the envelope kind:
// a delta-interval to merge, or an ack advancing a neighbor's progress.
func (r *causalReplicator[T]) receive(pkt *gossip.Packet) {
	e, err := decodeEnvelope(pkt.Data)
	if err != nil {
		_, span := r.tracer.Start(context.Background(), "antientropy.receive", trace.WithAttributes(
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.AddEvent("antientropy.envelope_decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	switch e.Kind {
	case kindDelta:
		r.receiveDelta(e, pkt)
	case kindAck:
		r.receiveAck(e, pkt)
	}
}

// receiveDelta merges an incoming delta-interval when it carries new
// information and acks the interval so the sender can advance this
// node's progress.
func (r *causalReplicator[T]) receiveDelta(e envelope, pkt *gossip.Packet) {
	ctx, span := r.tracer.Start(
		tracecontext.Extract(context.Background(), e.Carrier),
		"antientropy.receive_delta",
		trace.WithAttributes(
			attribute.String("antientropy.algorithm", "causal"),
			attribute.String("gossip.sender_address", pkt.From.String()),
			attribute.Int("gossip.message_bytes", len(pkt.Data)),
		),
	)
	defer span.End()

	delta, err := r.options.Decoder(e.Payload)
	if err != nil {
		span.AddEvent("antientropy.payload_decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		return
	}

	r.mtx.Lock()
	before := r.state
	merged := r.state.Merge(delta)
	changed := !merged.Equal(r.state)
	if changed {
		r.state = merged
		r.deltas[r.seq] = delta
		r.seq++
		r.persist(ctx)
	}
	after := r.state
	r.mtx.Unlock()

	span.SetAttributes(attribute.Bool("antientropy.changed", changed))

	if r.options.OnReceive != nil {
		r.options.OnReceive(ctx, before, delta, after)
	}

	ack := envelope{
		Kind:    kindAck,
		From:    r.options.Membership.LocalNode().ID,
		Seq:     e.Seq,
		Carrier: tracecontext.Inject(ctx),
	}

	msg, err := encodeEnvelope(ack)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	if err := r.options.Transport.SendTo(ctx, pkt.From, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func (r *causalReplicator[T]) receiveAck(e envelope, pkt *gossip.Packet) {
	_, span := r.tracer.Start(
		tracecontext.Extract(context.Background(), e.Carrier),
		"antientropy.receive_ack",
		trace.WithAttributes(
			attribute.String("antientropy.algorithm", "causal"),
			attribute.String("gossip.sender_address", pkt.From.String()),
			attribute.Int("gossip.message_bytes", len(pkt.Data)),
		),
	)
	defer span.End()

	if e.From == "" {
		span.AddEvent("antientropy.ack_missing_sender")
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.acks[e.From] = max(r.acks[e.From], e.Seq)
}

// persist writes the durable (seq, state) blob to the store.
func (r *causalReplicator[T]) persist(ctx context.Context) {
	ctx, span := r.tracer.Start(ctx, "antientropy.persist", trace.WithAttributes(
		attribute.String("antientropy.algorithm", "causal"),
		attribute.Int64("antientropy.seq", int64(r.seq)),
	))
	defer span.End()

	state, err := r.options.Encoder(r.state)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	blob, err := encodeSnapshot(snapshot{Seq: r.seq, State: state})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	if err := r.options.Store.Save(ctx, blob); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func (r *causalReplicator[T]) sendLoop(ctx context.Context) {
	ticker := time.NewTicker(r.options.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.send()
		}
	}
}

// send ships one gossip round to a single neighbor, chosen round-robin.
// It ships the delta-interval the neighbor has not yet acked, or the full
// state. We ship only when the neighbor is behind our sequence.
func (r *causalReplicator[T]) send() {
	peers := r.peers()
	if len(peers) == 0 {
		return
	}

	r.mtx.Lock()
	target := peers[r.round%uint64(len(peers))]
	r.round++
	acked := r.acks[target.id]
	seq := r.seq
	if acked >= seq {
		r.mtx.Unlock()
		return
	}
	state := r.state
	payload, full := r.deltaOrFull(acked, seq)
	r.mtx.Unlock()

	shipped := "full"
	if !full {
		shipped = "delta"
	}

	ctx, span := r.tracer.Start(context.Background(), "antientropy.gossip", trace.WithAttributes(
		attribute.String("antientropy.algorithm", "causal"),
		attribute.String("antientropy.neighbor", target.id),
		attribute.Int64("antientropy.acked", int64(acked)),
		attribute.Int64("antientropy.seq", int64(seq)),
		attribute.String("antientropy.delta_or_full", shipped),
		attribute.Int("antientropy.peer_count", len(peers)),
	))
	defer span.End()

	if r.options.OnSend != nil {
		r.options.OnSend(ctx, state)
	}

	data, err := r.options.Encoder(payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	e := envelope{
		Kind:    kindDelta,
		From:    r.options.Membership.LocalNode().ID,
		Seq:     seq,
		Payload: data,
		Carrier: tracecontext.Inject(ctx),
	}

	msg, err := encodeEnvelope(e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	span.SetAttributes(attribute.Int("antientropy.bytes_shipped", len(msg)))

	if err := r.options.Transport.SendTo(ctx, target.addr, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

type neighbor struct {
	id   string
	addr net.Addr
}

// peers resolves the transport address of every live, non-self member
// through the configured resolver.
func (r *causalReplicator[T]) peers() []neighbor {
	self := r.options.Membership.LocalNode().ID
	members := r.options.Membership.Members()

	out := make([]neighbor, 0, len(members))

	for _, m := range members {
		if m.ID == self || m.State != membership.Alive {
			continue
		}

		raw, ok := r.options.PeerAddress(m)
		if !ok {
			continue
		}

		addr, err := r.options.Transport.Resolve(raw)
		if err != nil {
			continue
		}

		out = append(out, neighbor{id: m.ID, addr: addr})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })

	return out
}

func (r *causalReplicator[T]) deltaOrFull(acked, seq uint64) (T, bool) {
	if len(r.deltas) == 0 {
		return r.state, true
	}

	// Suppose neighbor j we've never heard an ack from shows up: its ack
	// defaults to 0, but gc already dropped delta[0]. So, we ship full state.
	if _, ok := r.deltas[acked]; !ok {
		return r.state, true
	}

	// Suppose neighbor j has acked 2 and we still hold delta[2], ..., delta[seq-1].
	// In that case, we catch j up without shipping full state.
	var result T
	for s := acked; s < seq; s++ {
		result = result.Merge(r.deltas[s])
	}

	return result, false
}

func (r *causalReplicator[T]) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(r.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.gc()
		}
	}
}

// gc keeps the delta buffer from growing forever by dropping deltas every
// live neighbor holds.
func (r *causalReplicator[T]) gc() {
	live := r.liveMembers()

	r.mtx.Lock()
	for id := range r.acks {
		if _, ok := live[id]; !ok {
			delete(r.acks, id)
		}
	}

	if len(r.acks) == 0 {
		r.mtx.Unlock()
		return
	}

	minAck := uint64(math.MaxUint64)
	for _, acked := range r.acks {
		minAck = min(minAck, acked)
	}

	freed := 0
	for d := range r.deltas {
		if d < minAck {
			delete(r.deltas, d)
			freed++
		}
	}

	remaining := len(r.deltas)
	r.mtx.Unlock()

	if freed == 0 {
		return
	}

	_, span := r.tracer.Start(context.Background(), "antientropy.gc", trace.WithAttributes(
		attribute.String("antientropy.algorithm", "causal"),
		attribute.Int("antientropy.deltas_freed", freed),
		attribute.Int("antientropy.buffer_size", remaining),
	))
	span.End()
}

func (r *causalReplicator[T]) liveMembers() map[string]struct{} {
	members := r.options.Membership.Members()
	live := make(map[string]struct{}, len(members))
	for _, m := range members {
		if m.State == membership.Alive || m.State == membership.Suspect {
			live[m.ID] = struct{}{}
		}
	}
	return live
}
