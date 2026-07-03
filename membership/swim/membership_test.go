package swim_test

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/swim"
)

const (
	testProbeInterval = 50 * time.Millisecond
	testProbeTimeout  = 20 * time.Millisecond
	testIndirectK     = 2
	testSuspicionMult = 4
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestJoin_PropagatesMembership(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n0, g0 := newNode(t, "n0", nil)
	defer func() { _ = n0.Leave(ctx) }()

	n0Addr := g0.Addr(ctx).String()

	n0Watch, err := n0.Watch()
	require.NoError(t, err)

	n1, _ := newNode(t, "n1", []string{n0Addr})
	defer func() { _ = n1.Leave(ctx) }()

	n2, _ := newNode(t, "n2", []string{n0Addr})
	defer func() { _ = n2.Leave(ctx) }()

	// act + assert: convergence within a few probe intervals
	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1, n2}, 3),
		2*time.Second, 20*time.Millisecond,
		"cluster did not converge to 3 Alive members",
	)

	// assert: n0 emitted Join for both n1 and n2
	seen := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case ev := <-n0Watch:
			if ev.Type == membership.Join {
				seen[ev.Node.ID] = true
			}
		case <-deadline:
			break drain
		}
	}
	require.True(t, seen["n1"], "n0 expected Join for n1")
	require.True(t, seen["n2"], "n0 expected Join for n2")
}

func TestFailureDetection_StoppedTransportMarksFailed(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n0, g0 := newNode(t, "n0", nil)
	defer func() { _ = n0.Leave(ctx) }()
	n0Addr := g0.Addr(ctx).String()

	n1, _ := newNode(t, "n1", []string{n0Addr})
	defer func() { _ = n1.Leave(ctx) }()

	n2, g2 := newNode(t, "n2", []string{n0Addr})

	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1, n2}, 3),
		2*time.Second, 20*time.Millisecond,
		"cluster did not converge",
	)

	n0Watch, err := n0.Watch()
	require.NoError(t, err)
	n1Watch, err := n1.Watch()
	require.NoError(t, err)

	// act: simulate n2 crash
	require.NoError(t, g2.Stop(ctx))

	// assert: both n0 and n1 see Fail for n2
	failTimeout := testProbeInterval*time.Duration(testSuspicionMult) + 2*time.Second
	waitForEvent(t, n0Watch, membership.Fail, "n2", failTimeout)
	waitForEvent(t, n1Watch, membership.Fail, "n2", failTimeout)
}

func TestLeave_EmitsLeaveEvent(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n0, g0 := newNode(t, "n0", nil)
	defer func() { _ = n0.Leave(ctx) }()
	n0Addr := g0.Addr(ctx).String()

	n1, _ := newNode(t, "n1", []string{n0Addr})
	defer func() { _ = n1.Leave(ctx) }()

	n2, _ := newNode(t, "n2", []string{n0Addr})

	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1, n2}, 3),
		2*time.Second, 20*time.Millisecond,
		"cluster did not converge",
	)

	n0Watch, err := n0.Watch()
	require.NoError(t, err)
	n1Watch, err := n1.Watch()
	require.NoError(t, err)

	// act
	require.NoError(t, n2.Leave(ctx))

	// assert
	waitForEvent(t, n0Watch, membership.Leave, "n2", time.Second)
	waitForEvent(t, n1Watch, membership.Leave, "n2", time.Second)
}

type dropGossip struct {
	inner    gossip.Gossip
	dropping atomic.Bool
}

func (d *dropGossip) Addr(ctx context.Context) net.Addr { return d.inner.Addr(ctx) }
func (d *dropGossip) Listen(ctx context.Context) (<-chan *gossip.Packet, error) {
	return d.inner.Listen(ctx)
}
func (d *dropGossip) Stop(ctx context.Context) error     { return d.inner.Stop(ctx) }
func (d *dropGossip) Resolve(s string) (net.Addr, error) { return d.inner.Resolve(s) }

func (d *dropGossip) SendTo(ctx context.Context, addr net.Addr, msg []byte) error {
	if d.dropping.Load() {
		return nil
	}
	return d.inner.SendTo(ctx, addr, msg)
}

func (d *dropGossip) Broadcast(ctx context.Context, peers []net.Addr, msg []byte) error {
	if d.dropping.Load() {
		return nil
	}
	return d.inner.Broadcast(ctx, peers, msg)
}

func TestRecovery_RefutesSuspect(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n0, g0 := newNode(t, "n0", nil)
	defer func() { _ = n0.Leave(ctx) }()
	n0Addr := g0.Addr(ctx).String()

	n1, _ := newNode(t, "n1", []string{n0Addr})
	defer func() { _ = n1.Leave(ctx) }()

	innerG2, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	g2 := &dropGossip{inner: innerG2}

	n2, err := swim.New(
		membership.WithGossip(g2),
		membership.WithNodeID("n2"),
		swim.WithProbeInterval(testProbeInterval),
		swim.WithProbeTimeout(testProbeTimeout),
		swim.WithIndirectProbeCount(testIndirectK),
		swim.WithSuspicionMult(testSuspicionMult),
	)
	require.NoError(t, err)
	require.NoError(t, n2.Join(ctx, []string{n0Addr}))
	defer func() { _ = n2.Leave(ctx) }()

	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1, n2}, 3),
		2*time.Second, 20*time.Millisecond,
		"cluster did not converge",
	)

	// act: black-hole n2's outgoing
	g2.dropping.Store(true)

	// wait for n0 to flip n2 off Alive (Suspect or Failed)
	require.Eventually(t, func() bool {
		for _, m := range n0.Members() {
			if m.ID == "n2" && m.State != membership.Alive {
				return true
			}
		}
		return false
	}, testProbeInterval*time.Duration(testSuspicionMult)+2*time.Second, 20*time.Millisecond,
		"n0 never marked n2 non-Alive",
	)

	// unblock
	g2.dropping.Store(false)

	// assert: n0 sees n2 back to Alive after refute reaches it
	require.Eventually(t, func() bool {
		for _, m := range n0.Members() {
			if m.ID == "n2" && m.State == membership.Alive {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond,
		"n0 did not observe n2 back to Alive",
	)
}

func TestRejoin_AfterLeaveReclaimsIdentity(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n0, g0 := newNode(t, "n0", nil)
	defer func() { _ = n0.Leave(ctx) }()
	n0Addr := g0.Addr(ctx).String()

	n1a, _ := newNode(t, "n1", []string{n0Addr})

	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1a}, 2),
		2*time.Second, 20*time.Millisecond,
		"cluster did not converge",
	)

	// act: n1 gracefully leaves, and n0 records it Left.
	require.NoError(t, n1a.Leave(ctx))

	require.Eventually(t, func() bool {
		for _, m := range n0.Members() {
			if m.ID == "n1" && m.State == membership.Left {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond, "n0 never recorded n1 as Left")

	n1b, _ := newNode(t, "n1", []string{n0Addr})
	defer func() { _ = n1b.Leave(ctx) }()

	// assert: n0 reclaims the returned n1 and both re-converge to 2 Alive.
	require.Eventually(t,
		allAliveAt([]membership.Membership{n0, n1b}, 2),
		3*time.Second, 20*time.Millisecond,
		"n0 did not reclaim the returned n1 to Alive",
	)
}

func newNode(t *testing.T, id string, peers []string) (membership.Membership, gossip.Gossip) {
	t.Helper()

	g, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)

	m, err := swim.New(
		membership.WithGossip(g),
		membership.WithNodeID(id),
		swim.WithProbeInterval(testProbeInterval),
		swim.WithProbeTimeout(testProbeTimeout),
		swim.WithIndirectProbeCount(testIndirectK),
		swim.WithSuspicionMult(testSuspicionMult),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, m.Join(ctx, peers))

	return m, g
}

func waitForEvent(t *testing.T, ch <-chan membership.Event, want membership.EventType, nodeID string, timeout time.Duration) membership.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-ch:
			if ev.Type == want && ev.Node.ID == nodeID {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type=%d node=%s", want, nodeID)
			return membership.Event{}
		}
	}
}

func allAliveAt(nodes []membership.Membership, want int) func() bool {
	return func() bool {
		for _, n := range nodes {
			members := n.Members()
			if len(members) != want {
				return false
			}
			for _, mem := range members {
				if mem.State != membership.Alive {
					return false
				}
			}
		}
		return true
	}
}
