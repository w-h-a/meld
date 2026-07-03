package causal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/causal"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/crdt/pncounter"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/memory"
	memorygossip "github.com/w-h-a/meld/gossip/memory"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/stub"
	"github.com/w-h-a/meld/store"
	memorystore "github.com/w-h-a/meld/store/memory"
)

func TestCausal_PNCounter_NodesConverge(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net)}

	nodes := []antientropy.Replicator[pncounter.PNCounter]{
		newNode(t, "n1", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
		newNode(t, "n2", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
		newNode(t, "n3", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
	}

	ctx := context.Background()

	for _, n := range nodes {
		require.NoError(t, n.Start(ctx))
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, n := range nodes {
			_ = n.Stop(stopCtx)
		}
	}()

	// act
	nodes[0].Submit(nodes[0].State().IncrementDelta("n1"))
	nodes[0].Submit(nodes[0].State().IncrementDelta("n1"))
	nodes[1].Submit(nodes[1].State().DecrementDelta("n2"))
	nodes[2].Submit(nodes[2].State().IncrementDelta("n3"))

	// assert. All three converge to 2 - 1 + 1 = 2.
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			return n.State().Value() == 2
		}, time.Second, 10*time.Millisecond)
	}
}

func TestCausal_PNCounter_ConvergesUnderLossAndReorder(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net), memory.WithDropEvery(2), memory.WithReorder()}

	nodes := []antientropy.Replicator[pncounter.PNCounter]{
		newNode(t, "n1", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
		newNode(t, "n2", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
		newNode(t, "n3", cluster, pncounter.New(), encodePNCounter, decodePNCounter, memStore(t), opts...),
	}

	ctx := context.Background()

	for _, n := range nodes {
		require.NoError(t, n.Start(ctx))
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, n := range nodes {
			_ = n.Stop(stopCtx)
		}
	}()

	// act
	nodes[0].Submit(nodes[0].State().IncrementDelta("n1"))
	nodes[0].Submit(nodes[0].State().IncrementDelta("n1"))
	nodes[1].Submit(nodes[1].State().DecrementDelta("n2"))
	nodes[2].Submit(nodes[2].State().IncrementDelta("n3"))

	// assert. Half the messages drop and survivors arrive reordered. But an
	// unacked neighbor keeps A(j) low, so the sender re-ships the interval
	// each round (falling back to full state if the buffer can't cover it),
	// and a commutative, idempotent merge carries every node to
	// 2 - 1 + 1 = 2 eventually.
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			return n.State().Value() == 2
		}, 2*time.Second, 10*time.Millisecond)
	}
}

func TestCausal_PNCounter_RecoversAfterCrash(t *testing.T) {
	// arrange
	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
	}

	st := memStore(t)

	before := newNode(t, "n1", cluster, pncounter.New(), encodePNCounter, decodePNCounter, st, memory.WithNetwork(memory.NewNetwork()))

	ctx := context.Background()
	require.NoError(t, before.Start(ctx))

	// act
	before.Submit(before.State().IncrementDelta("n1"))
	before.Submit(before.State().IncrementDelta("n1"))
	before.Submit(before.State().IncrementDelta("n1"))

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, before.Stop(stopCtx))
	cancel()

	after := newNode(t, "n1", cluster, pncounter.New(), encodePNCounter, decodePNCounter, st, memory.WithNetwork(memory.NewNetwork()))
	require.NoError(t, after.Start(ctx))

	defer func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = after.Stop(c)
	}()

	// assert. Start reloaded (seq, state) from the store, so the restart comes
	// back at 3 even though it was built with an empty counter.
	require.Equal(t, int64(3), after.State().Value())
}

func encodePNCounter(pn pncounter.PNCounter) ([]byte, error) {
	return pn.Marshal()
}

func decodePNCounter(b []byte) (pncounter.PNCounter, error) {
	var pn pncounter.PNCounter
	err := pn.Unmarshal(b)
	return pn, err
}

func TestCausal_ORSet_NodesConverge(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net)}

	nodes := []antientropy.Replicator[orset.ORSet[string]]{
		newNode(t, "n1", cluster, orset.New[string](), encodeORSet, decodeORSet, memStore(t), opts...),
		newNode(t, "n2", cluster, orset.New[string](), encodeORSet, decodeORSet, memStore(t), opts...),
		newNode(t, "n3", cluster, orset.New[string](), encodeORSet, decodeORSet, memStore(t), opts...),
	}

	ctx := context.Background()

	for _, n := range nodes {
		require.NoError(t, n.Start(ctx))
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, n := range nodes {
			_ = n.Stop(stopCtx)
		}
	}()

	// act
	nodes[0].Submit(nodes[0].State().AddDelta("n1", "a"))
	nodes[1].Submit(nodes[1].State().AddDelta("n2", "b"))
	nodes[2].Submit(nodes[2].State().AddDelta("n3", "c"))

	// assert. All three converge to the union {a, b, c}.
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			s := n.State()
			return s.Contains("a") && s.Contains("b") && s.Contains("c")
		}, time.Second, 10*time.Millisecond)
	}
}

func encodeORSet(s orset.ORSet[string]) ([]byte, error) {
	return s.Marshal(crdt.StringEncode)
}

func decodeORSet(b []byte) (orset.ORSet[string], error) {
	var s orset.ORSet[string]
	err := s.Unmarshal(b, crdt.StringDecode)
	return s, err
}

func newNode[T crdt.Equatable[T]](
	t *testing.T,
	id string,
	cluster []membership.Node,
	initial T,
	encode antientropy.Encoder[T],
	decode antientropy.Decoder[T],
	st store.Store,
	transportOpts ...gossip.Option,
) antientropy.Replicator[T] {
	t.Helper()

	opts := append(
		[]gossip.Option{gossip.WithBindAddress(id)},
		transportOpts...,
	)

	transport, err := memorygossip.New(opts...)
	require.NoError(t, err)

	memb, err := stub.New(
		membership.WithNodeID(id),
		membership.WithAdvertiseAddress(id),
		stub.WithMembers(cluster...),
	)
	require.NoError(t, err)

	r, err := causal.New(
		antientropy.WithInitial(initial),
		antientropy.WithCodec(encode, decode),
		antientropy.WithTransport[T](transport),
		antientropy.WithMembership[T](memb),
		antientropy.WithStore[T](st),
		antientropy.WithInterval[T](10*time.Millisecond),
		causal.WithGCInterval[T](20*time.Millisecond),
	)
	require.NoError(t, err)

	return r
}

func memStore(t *testing.T) store.Store {
	t.Helper()
	s, err := memorystore.New()
	require.NoError(t, err)
	return s
}
