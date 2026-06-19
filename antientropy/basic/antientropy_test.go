package basic_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/antientropy/basic"
	"github.com/w-h-a/meld/crdt/gcounter"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/memory"
	"github.com/w-h-a/meld/membership"
	"github.com/w-h-a/meld/membership/stub"
)

func TestBasic_NodesConverge(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net)}

	nodes := []antientropy.Replicator[gcounter.GCounter]{
		newNode(t, "n1", cluster, opts...),
		newNode(t, "n2", cluster, opts...),
		newNode(t, "n3", cluster, opts...),
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

func TestBasic_ConvergesUnderLossAndReorder(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	cluster := []membership.Node{
		{ID: "n1", Address: "n1", State: membership.Alive},
		{ID: "n2", Address: "n2", State: membership.Alive},
		{ID: "n3", Address: "n3", State: membership.Alive},
	}

	opts := []gossip.Option{memory.WithNetwork(net), memory.WithDropEvery(2), memory.WithReorder()}

	nodes := []antientropy.Replicator[gcounter.GCounter]{
		newNode(t, "n1", cluster, opts...),
		newNode(t, "n2", cluster, opts...),
		newNode(t, "n3", cluster, opts...),
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

func encodeGCounter(g gcounter.GCounter) ([]byte, error) {
	return g.Marshal()
}

func decodeGCounter(b []byte) (gcounter.GCounter, error) {
	var g gcounter.GCounter
	err := g.Unmarshal(b)
	return g, err
}

func newNode(
	t *testing.T,
	id string,
	cluster []membership.Node,
	transportOpts ...gossip.Option,
) antientropy.Replicator[gcounter.GCounter] {
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
		antientropy.WithInitial(gcounter.New()),
		antientropy.WithCodec(encodeGCounter, decodeGCounter),
		antientropy.WithTransport[gcounter.GCounter](transport),
		antientropy.WithMembership[gcounter.GCounter](memb),
		antientropy.WithInterval[gcounter.GCounter](10*time.Millisecond),
	)
	require.NoError(t, err)

	return r
}
