// Package phi implements heartbeat-based membership with the phi accrual
// failure detector.
//
// References:
//   - Hayashibara et al., "The φ Accrual Failure Detector" (2004)
package phi

import (
	"context"
	"errors"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/util/tracecontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// phiMembership is the phi adapter. It owns the gossip
// transport's lifecycle from Join through Leave and runs three
// background routines: a sender that emits a heartbeat each
// interval, a receiver that folds arrivals into per-peer
// windows, and a checker that scores every peer and
// drives its state transitions.
type phiMembership struct {
	options membership.Options

	transport gossip.Gossip

	localID   string
	localAddr string
	localMeta map[string]string

	memberMtx sync.RWMutex
	members   map[string]*peerEntry

	seqNo atomic.Uint64

	eventCh chan membership.Event

	done    chan struct{}
	wg      sync.WaitGroup
	once    sync.Once
	started atomic.Bool

	rounds atomic.Uint64

	tracer trace.Tracer
}

func New(opts ...membership.Option) (membership.Membership, error) {
	options := membership.NewOptions(opts...)

	if options.Gossip == nil {
		return nil, errors.New("phi: WithGossip is required")
	}

	if options.NodeID == "" {
		return nil, errors.New("phi: WithNodeID is required")
	}

	meta := make(map[string]string, len(options.Meta))
	maps.Copy(meta, options.Meta)

	addr := options.AdvertiseAddress
	if addr == "" {
		addr = options.Gossip.Addr(context.Background()).String()
	}

	if membership.IsUnspecifiedHost(addr) {
		return nil, errors.New("phi: WithAdvertiseAddress required when gossip binds to 0.0.0.0 or [::]; peers cannot reach an unspecified address")
	}

	m := &phiMembership{
		options:   options,
		transport: options.Gossip,
		localID:   options.NodeID,
		localAddr: addr,
		localMeta: meta,
		members:   map[string]*peerEntry{},
		eventCh:   make(chan membership.Event, 64),
		done:      make(chan struct{}),
		tracer:    otel.Tracer("meld/membership/phi"),
	}

	return m, nil
}

// Join seeds self into members map, starts the sender, receiver, and
// checker routines, and introduces self to the seed peers with a
// heartbeat. Calling Join more than once returns an error.
func (m *phiMembership) Join(ctx context.Context, existing []string) error {
	if !m.started.CompareAndSwap(false, true) {
		return errors.New("phi: already joined")
	}

	ch, err := m.transport.Listen(ctx)
	if err != nil {
		m.started.Store(false)
		return err
	}

	m.memberMtx.Lock()
	m.members[m.localID] = &peerEntry{
		Address: m.localAddr,
		Meta:    m.localMeta,
		State:   membership.Alive,
	}
	m.memberMtx.Unlock()

	m.wg.Add(3)
	go m.runSender()
	go m.runReceiver(ch)
	go m.runChecker()

	for _, raw := range existing {
		addr, err := m.transport.Resolve(raw)
		if err != nil {
			continue
		}
		m.sendEnvelope(ctx, addr, envelope{
			Type: msgHeartbeat,
			From: nodeState{
				ID:      m.localID,
				Address: m.localAddr,
				Meta:    m.localMeta,
				State:   membership.Alive,
			},
			SeqNo: m.seqNo.Add(1),
		})
	}

	return nil
}

// runSender broadcasts a heartbeat to every Alive peer once per
// heartbeatInterval, until Leave closes the done ch.
func (m *phiMembership) runSender() {
	defer m.wg.Done()

	interval := heartbeatIntervalFrom(m.options.Context)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.sendHeartbeat()
		}
	}
}

// sendHeartbeat broadcasts one heartbeat to the currently Alive peers.
func (m *phiMembership) sendHeartbeat() {
	select {
	case <-m.done:
		return
	default:
	}

	m.memberMtx.RLock()
	peers := m.snapshotOfPeers()
	m.memberMtx.RUnlock()

	addrs := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	seq := m.seqNo.Add(1)

	ctx, span := m.tracer.Start(context.Background(), "phi.heartbeat.send", trace.WithAttributes(
		attribute.String("phi.node_id", m.localID),
		attribute.Int64("phi.heartbeat_seq", int64(seq)),
		attribute.Int("phi.peers_count", len(addrs)),
	))
	defer span.End()

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type:  msgHeartbeat,
		From:  m.selfView(),
		SeqNo: seq,
	})
}

// runReceiver drains the transport packet channel.
func (m *phiMembership) runReceiver(ch <-chan *gossip.Packet) {
	defer m.wg.Done()

	for {
		select {
		case <-m.done:
			return
		case pkt, ok := <-ch:
			if !ok {
				return
			}
			m.handlePacket(pkt)
		}
	}
}

// handlePacket merges the sender's self-view and dispatches by
// message type.
func (m *phiMembership) handlePacket(pkt *gossip.Packet) {
	e, err := decode(pkt.Data)
	if err != nil {
		_, span := m.tracer.Start(context.Background(), "phi.receive")
		span.AddEvent("phi.decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.End()
		return
	}

	ctx, span := m.tracer.Start(tracecontext.Extract(context.Background(), e.Carrier), "phi.receive", trace.WithAttributes(
		attribute.String("phi.node_id", m.localID),
		attribute.Int("phi.message_type", int(e.Type)),
		attribute.String("phi.sender", e.From.ID),
		attribute.Int64("phi.sender_seq", int64(e.SeqNo)),
	))
	defer span.End()

	switch e.Type {
	case msgHeartbeat:
		m.updatePeerWindow(ctx, e.From, time.Now())
	case msgLeave:
		m.applyLeft(ctx, e.From)
	}
}

// updatePeerWindow records a heartbeat's arrival for one peer.
//
// For a peer we already track, nextOnHeartbeat reports whether
// this heartbeat reclaims it from Failed or Left. A peer we have
// never seen, or one just reclaimed, starts fresh as an Alive
// entry with an empty window. Otherwise, the peer is one we already
// track, so we add the interval since its last arrival and advance
// lastArrival.
func (m *phiMembership) updatePeerWindow(ctx context.Context, from nodeState, now time.Time) {
	m.memberMtx.Lock()

	e, ok := m.members[from.ID]

	reclaimed := false
	if ok {
		_, reclaimed = nextOnHeartbeat(e.State)
	}

	if !ok || reclaimed {
		prev := membership.Alive
		if ok {
			prev = e.State
		}

		e := &peerEntry{
			Address:     from.Address,
			Meta:        from.Meta,
			State:       membership.Alive,
			lastArrival: now,
			window:      newSampleWindow(windowSizeFrom(m.options.Context)),
		}

		m.members[from.ID] = e
		node := toNode(from.ID, e)

		m.memberMtx.Unlock()

		m.recordTransition(ctx, prev, ok, node)
		return
	}

	interval := now.Sub(e.lastArrival)
	e.window.add(interval)
	e.lastArrival = now

	m.memberMtx.Unlock()
}

// applyLeft handles a graceful leave.
func (m *phiMembership) applyLeft(ctx context.Context, from nodeState) {
	m.memberMtx.Lock()

	e, ok := m.members[from.ID]
	if !ok || e.State == membership.Left {
		m.memberMtx.Unlock()
		return
	}

	prev := e.State
	e.State = membership.Left
	node := toNode(from.ID, e)

	m.memberMtx.Unlock()

	m.recordTransition(ctx, prev, true, node)
}

// runChecker ticks once per heartbeatInterval and runs one check round.
func (m *phiMembership) runChecker() {
	defer m.wg.Done()

	interval := heartbeatIntervalFrom(m.options.Context)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.checkRound()
		}
	}
}

// checkRound scores every Alive or Suspect peer once and records the ones
// that changed.
func (m *phiMembership) checkRound() {
	select {
	case <-m.done:
		return
	default:
	}

	round := m.rounds.Add(1)
	now := time.Now()
	low := phiLowThresholdFrom(m.options.Context)
	high := phiHighThresholdFrom(m.options.Context)

	changes, memberCount, peersChecked, suspectCount := m.scanPeers(now, low, high)

	ctx, span := m.tracer.Start(context.Background(), "phi.check.round", trace.WithAttributes(
		attribute.String("phi.node_id", m.localID),
		attribute.Int64("phi.round_number", int64(round)),
		attribute.Int("phi.member_count", memberCount),
		attribute.Int("phi.peersChecked", peersChecked),
		attribute.Int("phi.suspects_count", suspectCount),
	))
	defer span.End()

	for _, c := range changes {
		threshold := high
		if c.node.State == membership.Alive {
			threshold = low
		}
		m.recordTransition(ctx, c.from, true, c.node,
			attribute.Float64("phi.phi_value", c.phi),
			attribute.Float64("phi.threshold", threshold),
		)
	}
}

// scanPeers scores every Alive or Suspect peer. It skips self and Left peers,
// reaps any peer that has been Failed longer than the reap dwell, and scores
// the rest.
func (m *phiMembership) scanPeers(now time.Time, low float64, high float64) ([]peerChange, int, int, int) {
	m.memberMtx.Lock()
	defer m.memberMtx.Unlock()

	var changes []peerChange
	memberCount := len(m.members)
	peersChecked := 0
	suspectCount := 0

	for id, e := range m.members {
		if id == m.localID || e.State == membership.Left {
			continue
		}

		from, phi, reap := m.checkPeer(e, now, low, high)
		if reap {
			delete(m.members, id)
			continue
		}

		if from != membership.Failed {
			peersChecked++
		}
		if e.State == membership.Suspect {
			suspectCount++
		}
		if from != e.State {
			changes = append(changes, peerChange{
				from: from,
				node: toNode(id, e),
				phi:  phi,
			})
		}
	}

	return changes, memberCount, peersChecked, suspectCount
}

// checkPeer scores one peer and applies any state change in place. It returns
// the state the peer was in before, it's phi value, and whether it should be
// reaped.
func (m *phiMembership) checkPeer(e *peerEntry, now time.Time, low float64, high float64) (membership.State, float64, bool) {
	minStdDev := minStdDevFrom(m.options.Context)
	pause := acceptableHeartbeatPauseFrom(m.options.Context)
	dwell := suspectDwellFrom(m.options.Context)
	reapDwell := reapDwellFrom(m.options.Context)

	from := e.State
	phi := phiFromWindow(e.window, now.Sub(e.lastArrival), pause, minStdDev)

	dwellElapsed := now.Sub(e.suspectedAt) > dwell
	reapElapsed := now.Sub(e.failedAt) > reapDwell

	next, reap := nextOnTick(from, phi, low, high, dwellElapsed, reapElapsed)
	if reap {
		return from, phi, true
	}

	if next != e.State {
		e.State = next
		switch next {
		case membership.Suspect:
			e.suspectedAt = now
		case membership.Alive:
			e.suspectedAt = time.Time{}
		case membership.Failed:
			e.failedAt = now
		}
	}

	return from, phi, false
}

// recordTransition reports that a peer changed state.
func (m *phiMembership) recordTransition(ctx context.Context, from membership.State, wasPresent bool, node membership.Node, extra ...attribute.KeyValue) {
	attrs := append([]attribute.KeyValue{
		attribute.String("phi.node_id", m.localID),
		attribute.Int("phi.from_state", int(from)),
		attribute.Int("phi.to_state", int(node.State)),
		attribute.String("phi.peer", node.ID),
	}, extra...)

	_, span := m.tracer.Start(ctx, "phi.state_transition", trace.WithAttributes(attrs...))
	defer span.End()

	m.emit(membership.Event{
		Type: membership.DeriveEventType(wasPresent, from, node.State),
		Node: node,
	})
}

// emit is non-blocking. Slow watchers drop events rather than
// stalling the protocol. eventCh is intentionally never closed:
// AfterFunc and relay routines can outlive Leave's wg.Wait,
// and closing here would race with their emit calls.
func (m *phiMembership) emit(ev membership.Event) {
	select {
	case <-m.done:
		return
	default:
	}
	select {
	case m.eventCh <- ev:
	default:
	}
}

// selfView returns self as a nodeState for outbound
// heartbeats.
func (m *phiMembership) selfView() nodeState {
	return nodeState{
		ID:      m.localID,
		Address: m.localAddr,
		Meta:    m.localMeta,
		State:   membership.Alive,
	}
}

// Leave transitions self to Left, broadcasts that to the
// currently Alive peers, and shuts the adapter down.
// Subsequent calls are no-ops.
func (m *phiMembership) Leave(ctx context.Context) error {
	if !m.started.Load() {
		return errors.New("phi: not joined")
	}

	leftSelf := nodeState{
		ID:      m.localID,
		Address: m.localAddr,
		Meta:    m.localMeta,
		State:   membership.Left,
	}

	m.memberMtx.Lock()
	if e, ok := m.members[m.localID]; ok {
		e.State = membership.Left
	}
	peers := m.snapshotOfPeers()
	m.memberMtx.Unlock()

	addrs := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type: msgLeave,
		From: leftSelf,
	})

	var stopErr error
	m.once.Do(func() {
		close(m.done)
		stopErr = m.transport.Stop(ctx)
		m.wg.Wait()
		// eventCh is intentionally not closed (see emit).
	})

	return stopErr
}

// Members returns a snapshot of every known node, including self.
func (m *phiMembership) Members() []membership.Node {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()

	out := make([]membership.Node, 0, len(m.members))
	for id, n := range m.members {
		out = append(out, toNode(id, n))
	}

	return out
}

// LocalNode returns the view of self.
func (m *phiMembership) LocalNode() membership.Node {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()

	if e, ok := m.members[m.localID]; ok {
		return toNode(m.localID, e)
	}

	return membership.Node{
		ID:      m.localID,
		Address: m.localAddr,
		Meta:    m.localMeta,
		State:   membership.Alive,
	}
}

// Watch returns the membership event channel. The channel is
// intentionally never closed.
func (m *phiMembership) Watch() (<-chan membership.Event, error) {
	if !m.started.Load() {
		return nil, errors.New("phi: not joined")
	}

	return m.eventCh, nil
}

// snapshotOfPeers returns members in the Non-Left state,
// excluding self. Must be called with memberMtx held.
func (m *phiMembership) snapshotOfPeers() []*nodeState {
	out := make([]*nodeState, 0, len(m.members))
	for id, e := range m.members {
		if id == m.localID {
			continue
		}
		if e.State == membership.Left {
			continue
		}
		n := nodeState{
			ID:      id,
			Address: e.Address,
			Meta:    e.Meta,
			State:   e.State,
		}
		out = append(out, &n)
	}
	return out
}

// sendEnvelope injects the active trace context into carrier,
// encodes, and sends. Encode failures are caller input (bad
// envelope construction) so the span is bare. SendTo failures
// are runtime faults and mark the span error.
func (m *phiMembership) sendEnvelope(ctx context.Context, addr net.Addr, e envelope) {
	e.Carrier = tracecontext.Inject(ctx)

	data, err := encode(e)
	if err != nil {
		_, span := m.tracer.Start(ctx, "phi.send")
		span.AddEvent("phi.encode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	if err := m.transport.SendTo(ctx, addr, data); err != nil {
		_, span := m.tracer.Start(ctx, "phi.send")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
	}
}

// broadcastEnvelope injects the active trace context, encodes
// once, and fans out via gossip.Broadcast. Encode failures are
// caller input (bad envelope construction) so the span is bare.
// Broadcast failures are runtime faults and mark the span error.
func (m *phiMembership) broadcastEnvelope(ctx context.Context, peers []net.Addr, e envelope) {
	e.Carrier = tracecontext.Inject(ctx)

	data, err := encode(e)
	if err != nil {
		_, span := m.tracer.Start(ctx, "phi.broadcast")
		span.AddEvent("phi.encode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	if err := m.transport.Broadcast(ctx, peers, data); err != nil {
		_, span := m.tracer.Start(ctx, "phi.broadcast")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
	}
}

// toNode projects a peerEntry onto the public membership.Node type.
func toNode(id string, e *peerEntry) membership.Node {
	return membership.Node{
		ID:      id,
		Address: e.Address,
		Meta:    e.Meta,
		State:   e.State,
	}
}
