package udp_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/udp"
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSetPeers_DynamicAdd(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodes := make([]gossip.Gossip, 5)
	channels := make([]<-chan *gossip.Packet, 5)

	// node 0: sender with fanout covering max peer count
	n, err := udp.New(
		gossip.WithBindAddress("127.0.0.1:0"),
		gossip.WithFanout(4),
	)
	require.NoError(t, err)
	defer n.Stop(ctx)

	ch, err := n.Listen(ctx)
	require.NoError(t, err)

	nodes[0] = n
	channels[0] = ch

	for i := 1; i < 5; i++ {
		n, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
		require.NoError(t, err)
		defer n.Stop(ctx)

		ch, err := n.Listen(ctx)
		require.NoError(t, err)

		nodes[i] = n
		channels[i] = ch
	}

	// node 0 initially knows nodes 1-3 only
	err = nodes[0].SetPeers(ctx,
		nodes[1].Addr(ctx),
		nodes[2].Addr(ctx),
		nodes[3].Addr(ctx),
	)
	require.NoError(t, err)

	// act: broadcast before adding node 4
	err = nodes[0].Broadcast(ctx, []byte("before"))
	require.NoError(t, err)

	// assert: nodes 1-3 receive, node 4 does not
	for i := 1; i <= 3; i++ {
		select {
		case pkt := <-channels[i]:
			require.Equal(t, []byte("before"), pkt.Data)
		case <-ctx.Done():
			t.Fatalf("node %d: timed out waiting for message", i)
		}
	}

	select {
	case pkt := <-channels[4]:
		t.Fatalf("node 4 should not have received, got: %s", pkt.Data)
	case <-time.After(100 * time.Millisecond):
	}

	// act: add node 4 to peers and broadcast again
	err = nodes[0].SetPeers(ctx,
		nodes[1].Addr(ctx),
		nodes[2].Addr(ctx),
		nodes[3].Addr(ctx),
		nodes[4].Addr(ctx),
	)
	require.NoError(t, err)

	err = nodes[0].Broadcast(ctx, []byte("after"))
	require.NoError(t, err)

	// assert: all nodes 1-4 receive
	for i := 1; i <= 4; i++ {
		select {
		case pkt := <-channels[i]:
			require.Equal(t, []byte("after"), pkt.Data)
		case <-ctx.Done():
			t.Fatalf("node %d: timed out waiting for message", i)
		}
	}
}

func TestSendTo_DeliversToTargetPeer(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receiver, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer receiver.Stop(ctx)

	sender, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer sender.Stop(ctx)

	ch, err := receiver.Listen(ctx)
	require.NoError(t, err)

	want := []byte("point-to-point")

	// act
	err = sender.SendTo(ctx, receiver.Addr(ctx), want)
	require.NoError(t, err)

	// assert
	select {
	case pkt := <-ch:
		require.Equal(t, want, pkt.Data)
		require.Equal(t, sender.Addr(ctx).String(), pkt.From.String())
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestStop_ClosesChannel(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	node, err := udp.New()
	require.NoError(t, err)

	ch, err := node.Listen(ctx)
	require.NoError(t, err)

	// act
	err = node.Stop(ctx)
	require.NoError(t, err)

	// assert
	_, open := <-ch
	require.False(t, open)
}

func TestBroadcast_SmallMessage(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receiver, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer receiver.Stop(ctx)

	sender, err := udp.New(
		gossip.WithBindAddress("127.0.0.1:0"),
		gossip.WithPeers(receiver.Addr(ctx)),
	)
	require.NoError(t, err)
	defer sender.Stop(ctx)

	ch, err := receiver.Listen(ctx)
	require.NoError(t, err)

	want := []byte("hello gossip")

	// act
	err = sender.Broadcast(ctx, want)
	require.NoError(t, err)

	// assert
	select {
	case pkt := <-ch:
		require.Equal(t, want, pkt.Data)
		require.Equal(t, sender.Addr(ctx).String(), pkt.From.String())
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestBroadcast_ExceedsMTU(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mtu := 100

	sender, err := udp.New(udp.WithMTU(mtu))
	require.NoError(t, err)
	defer sender.Stop(ctx)

	oversized := make([]byte, mtu)

	// act
	err = sender.Broadcast(ctx, oversized)

	// assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds MTU")
}

func TestBroadcast_Fanout(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	receivers := make([]gossip.Gossip, 4)
	channels := make([]<-chan *gossip.Packet, 4)

	for i := range receivers {
		n, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
		require.NoError(t, err)
		defer n.Stop(ctx)

		ch, err := n.Listen(ctx)
		require.NoError(t, err)

		receivers[i] = n
		channels[i] = ch
	}

	peerAddrs := make([]net.Addr, len(receivers))
	for i, r := range receivers {
		peerAddrs[i] = r.Addr(ctx)
	}

	sender, err := udp.New(
		gossip.WithBindAddress("127.0.0.1:0"),
		gossip.WithPeers(peerAddrs...),
		gossip.WithFanout(2),
	)
	require.NoError(t, err)
	defer sender.Stop(ctx)

	// act
	err = sender.Broadcast(ctx, []byte("fanout"))
	require.NoError(t, err)

	// assert: exactly 2 of 4 receivers get the message
	received := 0
	for i := range channels {
		select {
		case <-channels[i]:
			received++
		case <-time.After(200 * time.Millisecond):
		}
	}

	require.Equal(t, 2, received)
}
