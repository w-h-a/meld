// Package swim implements the SWIM membership protocol.
//
// References:
//   - Das et al., "SWIM: Scalable Weakly-consistent Infection-style
//     Process Group Membership Protocol" (2002)
//   - hashicorp/memberlist for implementation patterns
package swim

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
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

// swimMembership is the SWIM adapter. It owns the gossip
// transport's lifecycle from Join through Leave and runs two
// background routines: a receiver that drains transport packets,
// and a prober that ticks once per probeInterval.
type swimMembership struct {
	options membership.Options

	transport gossip.Gossip

	localID   string
	localAddr string
	localMeta map[string]string

	localIncMtx sync.RWMutex
	localInc    uint64

	memberMtx sync.RWMutex
	members   map[string]nodeState

	suspectMtx    sync.RWMutex
	suspectTimers map[string]*time.Timer

	pendingMtx sync.RWMutex
	pendingAck map[uint64]chan context.Context
	seqNo      atomic.Uint64

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
		return nil, errors.New("swim: WithGossip is required")
	}

	if options.NodeID == "" {
		return nil, errors.New("swim: WithNodeID is required")
	}

	meta := make(map[string]string, len(options.Meta))
	maps.Copy(meta, options.Meta)

	addr := options.AdvertiseAddress
	if addr == "" {
		addr = options.Gossip.Addr(context.Background()).String()
	}

	if membership.IsUnspecifiedHost(addr) {
		return nil, errors.New("swim: WithAdvertiseAddress required when gossip binds to 0.0.0.0 or [::]; peers cannot reach an unspecified address")
	}

	m := &swimMembership{
		options:       options,
		transport:     options.Gossip,
		localID:       options.NodeID,
		localAddr:     addr,
		localMeta:     meta,
		members:       map[string]nodeState{},
		suspectTimers: map[string]*time.Timer{},
		pendingAck:    map[uint64]chan context.Context{},
		eventCh:       make(chan membership.Event, 64),
		done:          make(chan struct{}),
		tracer:        otel.Tracer("meld/membership/swim"),
	}

	return m, nil
}

// Join announces self to seed peers, starts the receiver and
// prober routines, and seeds the local members map with self.
// Calling Join more than once returns an error.
func (m *swimMembership) Join(ctx context.Context, existing []string) error {
	if !m.started.CompareAndSwap(false, true) {
		return errors.New("swim: already joined")
	}

	ch, err := m.transport.Listen(ctx)
	if err != nil {
		m.started.Store(false)
		return err
	}

	m.localIncMtx.Lock()
	m.localInc = uint64(time.Now().UnixNano())
	inc := m.localInc
	m.localIncMtx.Unlock()

	self := nodeState{
		ID:          m.localID,
		Address:     m.localAddr,
		Meta:        m.localMeta,
		State:       membership.Alive,
		Incarnation: inc,
	}

	m.memberMtx.Lock()
	m.members[m.localID] = self
	m.memberMtx.Unlock()

	m.wg.Add(2)
	go m.runReceiver(ch)
	go m.runProber()

	for _, raw := range existing {
		addr, err := m.transport.Resolve(raw)
		if err != nil {
			continue
		}
		m.sendEnvelope(ctx, addr, envelope{
			Type:   msgState,
			From:   self,
			Target: self,
		})
	}

	return nil
}

// runReceiver drains the transport packet channel.
func (m *swimMembership) runReceiver(ch <-chan *gossip.Packet) {
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
// message type
func (m *swimMembership) handlePacket(pkt *gossip.Packet) {
	e, err := decode(pkt.Data)
	if err != nil {
		_, span := m.tracer.Start(context.Background(), "swim.receive")
		span.AddEvent("swim.decode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
			attribute.String("gossip.sender_address", pkt.From.String()),
		))
		span.End()
		return
	}

	ctx, span := m.tracer.Start(tracecontext.Extract(context.Background(), e.Carrier), "swim.receive", trace.WithAttributes(
		attribute.String("swim.node_id", m.localID),
		attribute.Int("swim.message_type", int(e.Type)),
		attribute.String("swim.sender", e.From.ID),
		attribute.Int64("swim.sender_seq", int64(e.SeqNo)),
	))
	defer span.End()

	m.mergeAndEmit(ctx, e.From.ID, &e.From)

	if e.Type == msgState && e.Target.ID != "" && e.Target.ID != e.From.ID {
		m.mergeAndEmit(ctx, e.From.ID, &e.Target)
	}

	if e.Type == msgState && e.From.ID == e.Target.ID && e.From.ID != m.localID {
		m.disseminateSelfOriginated(ctx, e.From)
	}

	switch e.Type {
	// "Is receiver alive? Reply with my SeqNo."
	case msgPing:
		self := m.selfView()
		m.sendEnvelope(ctx, pkt.From, envelope{
			Type:  msgAck,
			From:  self,
			SeqNo: e.SeqNo,
		})
	// "Yes sender is alive, here's your SeqNo back."
	case msgAck:
		m.signalAck(ctx, e.SeqNo)
	// "Is C alive? Please ping C for me. Reply with my SeqNo."
	case msgPingReq:
		m.relayPingReq(ctx, pkt.From, e)
	// "C acked me. Here's the original SeqNo back."
	case msgIndirectAck:
		m.signalAck(ctx, e.SeqNo)
	}
}

// disseminateSelfOriginated forwards a self-originated msgState
// (Join announce, Leave broadcast, refute) one hop to our other
// alive peers.
func (m *swimMembership) disseminateSelfOriginated(ctx context.Context, learned nodeState) {
	self := m.selfView()

	m.memberMtx.RLock()
	peers := m.snapshotOfPeers()
	m.memberMtx.RUnlock()

	addrs := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		if p.ID == learned.ID {
			continue
		}
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	if len(addrs) == 0 {
		return
	}

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type:   msgState,
		From:   self,
		Target: learned,
	})
}

// signalAck delivers the receive ctx to a waiting prober for
// the given seq.
func (m *swimMembership) signalAck(ctx context.Context, seq uint64) {
	m.pendingMtx.Lock()
	ch, ok := m.pendingAck[seq]
	if ok {
		delete(m.pendingAck, seq)
	}
	m.pendingMtx.Unlock()
	if ok {
		ch <- ctx
	}
}

// relayPingReq forwards an indirect probe.
func (m *swimMembership) relayPingReq(ctx context.Context, requester net.Addr, original envelope) {
	if original.Target.ID == "" {
		return
	}

	addr, err := m.transport.Resolve(original.Target.Address)
	if err != nil {
		return
	}

	relaySeq := m.seqNo.Add(1)

	ch := m.registerPending(relaySeq)

	self := m.selfView()

	m.sendEnvelope(ctx, addr, envelope{
		Type:  msgPing,
		From:  self,
		SeqNo: relaySeq,
	})

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.deregisterPending(relaySeq)

		timeout := probeTimeoutFrom(m.options.Context)

		select {
		case <-m.done:
			return
		case <-time.After(timeout):
			return
		case ackCtx := <-ch:
			m.sendEnvelope(ackCtx, requester, envelope{
				Type:  msgIndirectAck,
				From:  m.selfView(),
				SeqNo: original.SeqNo,
			})
		}
	}()
}

// runProber ticks once per probeInterval and runs one probe round.
func (m *swimMembership) runProber() {
	defer m.wg.Done()

	interval := probeIntervalFrom(m.options.Context)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.probeRound()
		}
	}
}

// probeRound implements the SWIM failure-detection round:
// try direct ping, otherwise try indirect ping-req on timeout,
// and otherwise mark Suspect.
func (m *swimMembership) probeRound() {
	select {
	case <-m.done:
		return
	default:
	}

	round := m.rounds.Add(1)

	target, memberCount, suspectCount, probePoolSize := m.pickProbeTarget()

	ctx, span := m.tracer.Start(context.Background(), "swim.probe.round", trace.WithAttributes(
		attribute.String("swim.node_id", m.localID),
		attribute.Int64("swim.round_number", int64(round)),
		attribute.Int("swim.member_count", memberCount),
		attribute.Int("swim.probe_pool_size", probePoolSize),
		attribute.Int("swim.suspect_count", suspectCount),
	))
	defer span.End()

	if target.ID == "" {
		span.SetAttributes(attribute.String("swim.probe_result", "no_target"))
		return
	}

	span.SetAttributes(attribute.String("swim.probe_target", target.ID))

	self := m.selfView()

	if m.directProbe(ctx, target, self) {
		span.SetAttributes(attribute.String("swim.probe_result", "ack"))
		m.cancelSuspectTimer(target.ID)
		return
	}

	if m.indirectProbe(ctx, target, self) {
		span.SetAttributes(attribute.String("swim.probe_result", "indirect_ack"))
		m.cancelSuspectTimer(target.ID)
		return
	}

	span.SetAttributes(attribute.String("swim.probe_result", "timeout"))

	m.markSuspect(ctx, target)
}

// pickProbeTarget chooses a random non-self, non-Failed, non-Left
// node and returns it alongside member count, suspect count, and
// the effective probe pool size.
func (m *swimMembership) pickProbeTarget() (nodeState, int, int, int) {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()

	candidates := make([]nodeState, 0, len(m.members))
	suspectCount := 0
	for _, n := range m.members {
		if n.ID == m.localID {
			continue
		}
		if n.State == membership.Failed || n.State == membership.Left {
			continue
		}
		if n.State == membership.Suspect {
			suspectCount++
		}
		candidates = append(candidates, n)
	}

	pool := len(candidates)
	if pool == 0 {
		return nodeState{}, len(m.members), suspectCount, 0
	}

	return candidates[rand.IntN(len(candidates))], len(m.members), suspectCount, pool
}

// directProbe sends a msgPing and waits up to probeTimeout for
// the matching msgAck.
func (m *swimMembership) directProbe(ctx context.Context, target, self nodeState) bool {
	addr, err := m.transport.Resolve(target.Address)
	if err != nil {
		return false
	}

	seq := m.seqNo.Add(1)
	ch := m.registerPending(seq)
	defer m.deregisterPending(seq)

	m.sendEnvelope(ctx, addr, envelope{
		Type:  msgPing,
		From:  self,
		SeqNo: seq,
	})

	timeout := probeTimeoutFrom(m.options.Context)
	select {
	case <-m.done:
		return false
	case <-time.After(timeout):
		return false
	case ackCtx := <-ch:
		_ = ackCtx
		return true
	}
}

// indirectProbe asks k relays to ping the target on our behalf
// and waits the remainder of the probe interval for any
// msgIndirectAck.
func (m *swimMembership) indirectProbe(ctx context.Context, target, self nodeState) bool {
	k := indirectProbeCountFrom(m.options.Context)
	relays := m.pickRelays(target.ID, k)
	if len(relays) == 0 {
		return false
	}

	seq := m.seqNo.Add(1)
	ch := m.registerPending(seq)
	defer m.deregisterPending(seq)

	for _, relay := range relays {
		addr, err := m.transport.Resolve(relay.Address)
		if err != nil {
			continue
		}
		m.sendEnvelope(ctx, addr, envelope{
			Type:   msgPingReq,
			From:   self,
			Target: target,
			SeqNo:  seq,
		})
	}

	interval := probeIntervalFrom(m.options.Context)
	timeout := probeTimeoutFrom(m.options.Context)
	remaining := interval - timeout
	if remaining <= 0 {
		remaining = timeout
	}

	select {
	case <-m.done:
		return false
	case <-time.After(remaining):
		return false
	case ackCtx := <-ch:
		_ = ackCtx
		return true
	}
}

// pickRelays returns up to k random Alive peers, excluding self
// and the probe target.
func (m *swimMembership) pickRelays(excludeID string, k int) []nodeState {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()

	candidates := make([]nodeState, 0, len(m.members))
	for _, n := range m.members {
		if n.ID == m.localID || n.ID == excludeID {
			continue
		}
		if n.State != membership.Alive {
			continue
		}
		candidates = append(candidates, n)
	}

	if len(candidates) <= k {
		return candidates
	}

	for i := 0; i < k; i++ {
		j := i + rand.IntN(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}

	return candidates[:k]
}

// markSuspect transitions a target to Suspect at its current
// incarnation via apply, which also schedules the failure timer.
// Disseminates the opinion to Alive peers and the suspect itself
// so that target can self-refute if it's actually alive.
func (m *swimMembership) markSuspect(ctx context.Context, target nodeState) {
	target.State = membership.Suspect
	m.mergeAndEmit(ctx, m.localID, &target)

	self := m.selfView()

	m.memberMtx.RLock()
	peers := m.snapshotOfPeers()
	m.memberMtx.RUnlock()

	addrs := make([]net.Addr, 0, len(peers)+1)
	if addr, err := m.transport.Resolve(target.Address); err == nil {
		addrs = append(addrs, addr)
	}

	for _, p := range peers {
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type:   msgState,
		From:   self,
		Target: target,
	})
}

// registerPending records a buffered channel for a probe's
// awaited ack ctx and returns it.
func (m *swimMembership) registerPending(seq uint64) chan context.Context {
	ch := make(chan context.Context, 1)
	m.pendingMtx.Lock()
	m.pendingAck[seq] = ch
	m.pendingMtx.Unlock()
	return ch
}

// deregisterPending removes the channel for seq.
func (m *swimMembership) deregisterPending(seq uint64) {
	m.pendingMtx.Lock()
	delete(m.pendingAck, seq)
	m.pendingMtx.Unlock()
}

// mergeAndEmit applies incoming state through state.apply, persists the
// result, schedules or cancels the suspect timer, emits an event,
// and triggers self-refutation when the merged self-view became
// Suspect or Failed.
func (m *swimMembership) mergeAndEmit(ctx context.Context, reporter string, incoming *nodeState) {
	if incoming == nil || incoming.ID == "" {
		return
	}

	m.memberMtx.Lock()
	local, wasPresent := m.members[incoming.ID]
	if !wasPresent {
		local = nodeState{ID: incoming.ID}
	}
	next, changed := apply(local, *incoming)
	if !changed {
		m.memberMtx.Unlock()
		return
	}
	m.members[incoming.ID] = next
	m.memberMtx.Unlock()

	if incoming.ID == m.localID && (next.State == membership.Suspect || next.State == membership.Failed) {
		m.refuteSelf(ctx, next.Incarnation)
		return
	}

	_, span := m.tracer.Start(ctx, "swim.state_transition", trace.WithAttributes(
		attribute.String("swim.node_id", incoming.ID),
		attribute.Int("swim.from_state", int(local.State)),
		attribute.Int("swim.to_state", int(next.State)),
		attribute.Int64("swim.incarnation", int64(next.Incarnation)),
		attribute.String("swim.reporter", reporter),
	))
	span.End()

	switch next.State {
	case membership.Suspect:
		m.scheduleSuspectTimer(ctx, reporter, incoming.ID, next.Incarnation)
	default:
		m.cancelSuspectTimer(incoming.ID)
	}

	m.emit(membership.Event{
		Type: membership.DeriveEventType(wasPresent, local.State, next.State),
		Node: toNode(next),
	})
}

// refuteSelf bumps localInc above the observed value, rewrites
// self as Alive at the new incarnation, and broadcasts msgState
// to currently Alive peers. Held locks are released before
// transport I/O per the lock-order invariant.
func (m *swimMembership) refuteSelf(ctx context.Context, observedInc uint64) {
	m.localIncMtx.Lock()
	if observedInc+1 <= m.localInc {
		m.localIncMtx.Unlock()
		return
	}
	m.localInc = observedInc + 1
	inc := m.localInc
	m.localIncMtx.Unlock()

	refuted := nodeState{
		ID:          m.localID,
		Address:     m.localAddr,
		Meta:        m.localMeta,
		State:       membership.Alive,
		Incarnation: inc,
	}

	m.memberMtx.Lock()
	m.members[m.localID] = refuted
	peers := m.snapshotOfPeers()
	m.memberMtx.Unlock()

	m.cancelSuspectTimer(m.localID)

	m.emit(membership.Event{
		Type: membership.Update,
		Node: toNode(refuted),
	})

	addrs := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type:   msgState,
		From:   refuted,
		Target: refuted,
	})
}

// scheduleSuspectTimer arms an AfterFunc that promotes a Suspect
// node to Failed if no refutation arrives within
// probeInterval x suspicionMult. The armed incarnation is closed
// over so a stale callback cannot kill a node that was refuted and
// later re-suspected at a higher incarnation.
func (m *swimMembership) scheduleSuspectTimer(ctx context.Context, reporter string, id string, incarnation uint64) {
	interval := probeIntervalFrom(m.options.Context)
	mult := suspicionMultFrom(m.options.Context)
	timeout := interval * time.Duration(mult)

	timer := time.AfterFunc(timeout, func() {
		expireCtx, span := m.tracer.Start(context.Background(), "swim.suspicion.expire", trace.WithAttributes(
			attribute.String("swim.suspected_node", id),
			attribute.Int64("swim.suspected_incarnation", int64(incarnation)),
			attribute.Int64("swim.suspicion_timeout_ms", timeout.Milliseconds()),
		))
		defer span.End()
		m.expireSuspect(expireCtx, id, incarnation)
	})

	m.suspectMtx.Lock()
	if existing, ok := m.suspectTimers[id]; ok {
		existing.Stop()
	}
	m.suspectTimers[id] = timer
	m.suspectMtx.Unlock()

	trace.SpanFromContext(ctx).AddEvent("swim.suspicion.start", trace.WithAttributes(
		attribute.String("swim.suspected_node", id),
		attribute.String("swim.reporter", reporter),
		attribute.Int64("swim.suspected_incarnation", int64(incarnation)),
		attribute.Int64("swim.suspicion_timeout_ms", timeout.Milliseconds()),
	))
}

// cancelSuspectTimer stops and forgets the timer for id, if any.
func (m *swimMembership) cancelSuspectTimer(id string) {
	m.suspectMtx.Lock()
	if t, ok := m.suspectTimers[id]; ok {
		t.Stop()
		delete(m.suspectTimers, id)
	}
	m.suspectMtx.Unlock()
}

// expireSuspect promotes a still-Suspect node to Failed. If the
// node was refuted in the meantime, this is a no-op.
func (m *swimMembership) expireSuspect(ctx context.Context, id string, expectedInc uint64) {
	m.memberMtx.RLock()
	current, ok := m.members[id]
	m.memberMtx.RUnlock()
	if !ok || current.State != membership.Suspect || current.Incarnation != expectedInc {
		return
	}

	failed := current
	failed.State = membership.Failed
	m.mergeAndEmit(ctx, m.localID, &failed)

	self := m.selfView()

	m.memberMtx.RLock()
	peers := m.snapshotOfPeers()
	m.memberMtx.RUnlock()

	addrs := make([]net.Addr, 0, len(peers)+1)
	if addr, err := m.transport.Resolve(failed.Address); err == nil {
		addrs = append(addrs, addr)
	}

	for _, p := range peers {
		addr, err := m.transport.Resolve(p.Address)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	m.broadcastEnvelope(ctx, addrs, envelope{
		Type:   msgState,
		From:   self,
		Target: failed,
	})
}

// emit is non-blocking. Slow watchers drop events rather than
// stalling the protocol. eventCh is intentionally never closed:
// AfterFunc and relay routines can outlive Leave's wg.Wait,
// and closing here would race with their emit calls.
func (m *swimMembership) emit(ev membership.Event) {
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

// selfView returns the current self entry from the members map.
func (m *swimMembership) selfView() nodeState {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()
	return m.members[m.localID]
}

// Leave bumps self incarnation, transitions self to Left, broadcasts
// the Left state to currently Alive peers, and shuts down the adapter.
// Subsequent calls are no-ops.
func (m *swimMembership) Leave(ctx context.Context) error {
	if !m.started.Load() {
		return errors.New("swim: not joined")
	}

	m.localIncMtx.Lock()
	m.localInc++
	inc := m.localInc
	m.localIncMtx.Unlock()

	leftSelf := nodeState{
		ID:          m.localID,
		Address:     m.localAddr,
		Meta:        m.localMeta,
		State:       membership.Left,
		Incarnation: inc,
	}

	m.memberMtx.Lock()
	m.members[m.localID] = leftSelf
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
		Type:   msgState,
		From:   leftSelf,
		Target: leftSelf,
	})

	var stopErr error
	m.once.Do(func() {
		close(m.done)
		m.suspectMtx.Lock()
		for _, t := range m.suspectTimers {
			t.Stop()
		}
		m.suspectMtx.Unlock()

		stopErr = m.transport.Stop(ctx)
		m.wg.Wait()
		// eventCh is intentionally not closed (see emit).
	})

	return stopErr
}

// Members returns a snapshot of every known node, including self.
func (m *swimMembership) Members() []membership.Node {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()

	out := make([]membership.Node, 0, len(m.members))
	for _, n := range m.members {
		out = append(out, toNode(n))
	}

	return out
}

// LocalNode returns the view of self.
func (m *swimMembership) LocalNode() membership.Node {
	m.memberMtx.RLock()
	defer m.memberMtx.RUnlock()
	n := m.members[m.localID]
	return toNode(n)
}

// Watch returns the membership event channel. The channel is
// intentionally never closed.
func (m *swimMembership) Watch() (<-chan membership.Event, error) {
	if !m.started.Load() {
		return nil, errors.New("swim: not joined")
	}

	return m.eventCh, nil
}

// snapshotOfPeers returns members in the Alive state,
// excluding self. Must be called with memberMtx held.
func (m *swimMembership) snapshotOfPeers() []*nodeState {
	out := make([]*nodeState, 0, len(m.members))
	for _, n := range m.members {
		if n.ID == m.localID {
			continue
		}
		if n.State != membership.Alive {
			continue
		}
		nodeCopy := n
		out = append(out, &nodeCopy)
	}
	return out
}

// sendEnvelope injects the active trace context into the carrier,
// encodes, and sends. Encode failures are caller input (bad
// envelope construction) so the span is bare. SendTo failures
// are runtime faults and mark the span error.
func (m *swimMembership) sendEnvelope(ctx context.Context, addr net.Addr, e envelope) {
	e.Carrier = tracecontext.Inject(ctx)

	data, err := encode(e)
	if err != nil {
		_, span := m.tracer.Start(ctx, "swim.send")
		span.AddEvent("swim.encode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	if err := m.transport.SendTo(ctx, addr, data); err != nil {
		_, span := m.tracer.Start(ctx, "swim.send")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
	}
}

// broadcastEnvelope injects the active trace context, encodes
// once, and fans out via gossip.Broadcast. Encode failures are
// caller input (bad envelope construction) so the span is bare.
// Broadcast failures are runtime faults and mark the span error.
func (m *swimMembership) broadcastEnvelope(ctx context.Context, peers []net.Addr, e envelope) {
	e.Carrier = tracecontext.Inject(ctx)

	data, err := encode(e)
	if err != nil {
		_, span := m.tracer.Start(ctx, "swim.broadcast")
		span.AddEvent("swim.encode_error", trace.WithAttributes(
			attribute.String("error.message", err.Error()),
		))
		span.End()
		return
	}

	if err := m.transport.Broadcast(ctx, peers, data); err != nil {
		_, span := m.tracer.Start(ctx, "swim.broadcast")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
	}
}

// toNode projects nodeState onto the public membership.Node type.
func toNode(n nodeState) membership.Node {
	return membership.Node{
		ID:      n.ID,
		Address: n.Address,
		Meta:    n.Meta,
		State:   n.State,
	}
}
