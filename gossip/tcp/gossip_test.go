package tcp_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/tcp"
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestBroadcast_SmallMessage(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receiver, err := tcp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer receiver.Stop(ctx)

	sender, err := tcp.New(gossip.WithPeers(receiver.Addr(ctx).String()))
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
	case got := <-ch:
		require.Equal(t, want, got)
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestBroadcast_LargeMessage(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receiver, err := tcp.New(gossip.WithBindAddress("127.0.0.1:0"))
	require.NoError(t, err)
	defer receiver.Stop(ctx)

	sender, err := tcp.New(gossip.WithPeers(receiver.Addr(ctx).String()))
	require.NoError(t, err)
	defer sender.Stop(ctx)

	ch, err := receiver.Listen(ctx)
	require.NoError(t, err)

	want := make([]byte, 4096)
	for i := range want {
		want[i] = byte(i % 256)
	}

	// act
	err = sender.Broadcast(ctx, want)
	require.NoError(t, err)

	// assert
	select {
	case got := <-ch:
		require.Equal(t, want, got)
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestStop_ClosesChannel(t *testing.T) {
	// arrange
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	node, err := tcp.New()
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
