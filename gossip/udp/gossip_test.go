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

func TestBroadcast_SendsToAllProvidedPeers(t *testing.T) {
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

	sender, err := udp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer sender.Stop(ctx)

	want := []byte("fanout")

	// act
	err = sender.Broadcast(ctx, peerAddrs, want)
	require.NoError(t, err)

	// assert: every receiver gets the message and sees sender's address
	for i := range channels {
		select {
		case pkt := <-channels[i]:
			require.Equal(t, want, pkt.Data)
			require.Equal(t, sender.Addr(ctx).String(), pkt.From.String())
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("receiver %d: timed out waiting for message", i)
		}
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
	err = sender.Broadcast(ctx, nil, oversized)

	// assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds MTU")
}
