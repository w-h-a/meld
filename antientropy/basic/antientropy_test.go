package basic_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/gcounter"
	"github.com/w-h-a/meld/crdt/lwwregister"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/memory"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/stub"
)

func TestBasic_GCounter_NodesConverge(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net)}

	nodes := []antientropy.Replicator[gcounter.GCounter]{
		newNode(t, "n1", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
		newNode(t, "n2", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
		newNode(t, "n3", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
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
	nodes[1].Submit(nodes[1].State().IncrementDelta("n2"))
	nodes[2].Submit(nodes[2].State().IncrementDelta("n3"))

	// assert. All three converge to the total, 2 + 1 + 1 = 4
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			return n.State().Value() == 4
		}, time.Second, 10*time.Millisecond)
	}
}

func TestBasic_GCounter_ConvergesUnderLossAndReorder(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net), memory.WithDropEvery(2), memory.WithReorder()}

	nodes := []antientropy.Replicator[gcounter.GCounter]{
		newNode(t, "n1", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
		newNode(t, "n2", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
		newNode(t, "n3", cluster, gcounter.New(), encodeGCounter, decodeGCounter, opts...),
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
	nodes[1].Submit(nodes[1].State().IncrementDelta("n2"))
	nodes[2].Submit(nodes[2].State().IncrementDelta("n3"))

	// assert. Half the messages drop and the survivors arrive reordered,
	// yet the periodic full-state ship plus a commutative, idempotent
	// merge still carry every node to the total, 2 + 1 + 1 = 4, eventually.
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			return n.State().Value() == 4
		}, 2*time.Second, 10*time.Millisecond)
	}
}

func TestBasic_LWWRegister_NodesConverge(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net)}

	nodes := []antientropy.Replicator[lwwregister.LWWRegister[string]]{
		newNode(t, "n1", cluster, lwwregister.New[string](), encodeLWWRegister, decodeLWWRegister, opts...),
		newNode(t, "n2", cluster, lwwregister.New[string](), encodeLWWRegister, decodeLWWRegister, opts...),
		newNode(t, "n3", cluster, lwwregister.New[string](), encodeLWWRegister, decodeLWWRegister, opts...),
	}

	nodes[0].Submit(nodes[0].State().Set("n1", "a"))
	nodes[1].Submit(nodes[1].State().Set("n2", "b"))
	nodes[2].Submit(nodes[2].State().Set("n3", "c"))

	ctx := context.Background()

	// act
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

	// assert. The tags tie at counter 1, so the Writer breaks the tie.
	// "n3" is the lexical max, so the basic node carries every replica to
	// its value, "c", by joining full states.
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			return n.State().Value() == "c"
		}, time.Second, 10*time.Millisecond)
	}
}

func encodeGCounter(g gcounter.GCounter) ([]byte, error) {
	return g.Marshal()
}

func decodeGCounter(b []byte) (gcounter.GCounter, error) {
	var g gcounter.GCounter
	err := g.Unmarshal(b)
	return g, err
}

func encodeLWWRegister(lww lwwregister.LWWRegister[string]) ([]byte, error) {
	return lww.Marshal(func(s string) ([]byte, error) {
		return []byte(s), nil
	})
}

func decodeLWWRegister(b []byte) (lwwregister.LWWRegister[string], error) {
	var lww lwwregister.LWWRegister[string]
	err := lww.Unmarshal(b, func(b []byte) (string, error) {
		return string(b), nil
	})
	return lww, err
}

func newNode[T crdt.Mergeable[T]](
	t *testing.T,
	id string,
	cluster []membership.Node,
	initial T,
	encode antientropy.Encoder[T],
	decode antientropy.Decoder[T],
	transportOpts ...gossip.Option,
) antientropy.Replicator[T] {
	t.Helper()

	opts := append(
		[]gossip.Option{gossip.WithBindAddress(id)},
		transportOpts...,
	)

	transport, err := memory.New(opts...)
	require.NoError(t, err)

	memb, err := stub.New(
		membership.WithNodeID(id),
		membership.WithAdvertiseAddress(id),
		stub.WithMembers(cluster...),
	)
	require.NoError(t, err)

	r, err := basic.New(
		antientropy.WithInitial(initial),
		antientropy.WithCodec(encode, decode),
		antientropy.WithTransport[T](transport),
		antientropy.WithMembership[T](memb),
		antientropy.WithInterval[T](10*time.Millisecond),
	)
	require.NoError(t, err)

	return r
}
