package basic

import (
	"context"
	"errors"
	"net"
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

// basicReplicator is the basic anti-entropy replicator for a CRDT.
// Every gossip round it ships the deltas buffered since the last round,
// or the full state when none were buffered, and merges whatever it
// receives.
type basicReplicator[T crdt.Mergeable[T]] struct {
	options    antientropy.Options[T]
	transitive bool

	mtx      sync.RWMutex
	state    T
	deltas   T
	buffered bool

	cancel context.CancelFunc
	wg     sync.WaitGroup

	tracer trace.Tracer
}

func New[T crdt.Mergeable[T]](opts ...antientropy.Option[T]) (antientropy.Replicator[T], error) {
	options := antientropy.NewOptions(opts...)

	switch {
	case options.Encoder == nil || options.Decoder == nil:
		return nil, errors.New("basic: a codec is required")
	case options.Transport == nil:
		return nil, errors.New("basic: a transport is required")
	case options.Membership == nil:
		return nil, errors.New("basic: a membership source is required")
	}

	r := &basicReplicator[T]{
		options:    options,
		transitive: transitiveFrom(options.Context),
		mtx:        sync.RWMutex{},
		state:      options.Initial,
		tracer:     otel.Tracer("meld/antientropy/basic"),
	}

	return r, nil
}

// Submit joins a locally produced delta into the state and queues it for
// the next gossip round.
func (r *basicReplicator[T]) Submit(delta T) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.state = r.state.Merge(delta)
	r.deltas = r.deltas.Merge(delta)
	r.buffered = true
}

// State returns the current converged CRDT state.
func (r *basicReplicator[T]) State() T {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return r.state
}

// Start opens the transport listener and launches the receive and gossip
// loops.
func (r *basicReplicator[T]) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)

	ch, err := r.options.Transport.Listen(loopCtx)
	if err != nil {
		cancel()
		return err
	}

	r.cancel = cancel
	r.wg.Add(2)

	go func() {
		defer r.wg.Done()
		r.receiveLoop(loopCtx, ch)
	}()

	go func() {
		defer r.wg.Done()
		r.sendLoop(loopCtx)
	}()

	return nil
}

// Stop cancels the loops and waits for them to exit, or returns the
// context's error if they do not exit in time.
func (r *basicReplicator[T]) Stop(ctx context.Context) error {
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

func (r *basicReplicator[T]) receiveLoop(ctx context.Context, ch <-chan *gossip.Packet) {
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

// receive decodes one gossip.Packet and merges the payload into the
// state.
func (r *basicReplicator[T]) receive(pkt *gossip.Packet) {
	e, err := decode(pkt.Data)
	if err != nil {
		_, span := r.tracer.Start(context.Background(), "antientropy.receive", trace.WithAttributes(
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.AddEvent("antientropy.from_decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	ctx, span := r.tracer.Start(
		tracecontext.Extract(context.Background(), e.Carrier),
		"antientropy.receive",
		trace.WithAttributes(
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
	r.state = r.state.Merge(delta)
	after := r.state
	if r.transitive {
		r.deltas = r.deltas.Merge(delta)
		r.buffered = true
	}
	r.mtx.Unlock()

	if r.options.OnReceive != nil {
		r.options.OnReceive(ctx, before, delta, after)
	}
}

func (r *basicReplicator[T]) sendLoop(ctx context.Context) {
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

// send ships one gossip round: the delta-group if this round buffered
// anything; otherwise, the full state.
func (r *basicReplicator[T]) send() {
	r.mtx.Lock()
	state := r.state
	payload, full := state, true
	if r.buffered {
		payload, full = r.deltas, false
	}
	var empty T
	r.deltas = empty
	r.buffered = false
	r.mtx.Unlock()

	shipped := "full"
	if !full {
		shipped = "delta"
	}

	peers := r.peers()

	ctx, span := r.tracer.Start(context.Background(), "antientropy.gossip", trace.WithAttributes(
		attribute.String("antientropy.algorithm", "basic"),
		attribute.String("antientropy.delta_or_full", shipped),
		attribute.Int("antientropy.peer_count", len(peers)),
	))
	defer span.End()

	if r.options.OnSend != nil {
		r.options.OnSend(ctx, state)
	}

	if len(peers) == 0 {
		return
	}

	data, err := r.options.Encoder(payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	e := envelope{Payload: data, Carrier: tracecontext.Inject(ctx)}

	msg, err := encode(e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	span.SetAttributes(attribute.Int("antientropy.bytes_shipped", len(msg)))

	if err := r.options.Transport.Broadcast(ctx, peers, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// peers resolves the transport address of every live, non-self member
// through the configured resolver.
func (r *basicReplicator[T]) peers() []net.Addr {
	self := r.options.Membership.LocalNode().ID
	members := r.options.Membership.Members()
	addrs := make([]net.Addr, 0, len(members))

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

		addrs = append(addrs, addr)
	}

	return addrs
}
