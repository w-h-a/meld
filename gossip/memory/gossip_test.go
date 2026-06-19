package memory_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/gossip/memory"
)

func TestMemory_DropsHalfAndSwapsSurvivors(t *testing.T) {
	// arrange
	net := memory.NewNetwork()

	rx, err := memory.New(gossip.WithBindAddress("rx"), memory.WithNetwork(net))
	require.NoError(t, err)

	tx, err := memory.New(
		gossip.WithBindAddress("tx"),
		memory.WithNetwork(net),
		memory.WithDropEvery(2),
		memory.WithReorder(),
	)
	require.NoError(t, err)

	ctx := context.Background()

	inbox, err := rx.Listen(ctx)
	require.NoError(t, err)

	to, err := tx.Resolve("rx")
	require.NoError(t, err)

	// act. Send four messages, 1, 2, 3, 4, to the same destination.
	for _, payload := range [][]byte{{1}, {2}, {3}, {4}} {
		require.NoError(t, tx.SendTo(ctx, to, payload))
	}

	// assert. Loss drops every second message, so 2 and 4 never arrive,
	// leaving exactly two packets buffered. Reorder swaps each adjacent
	// pair of survivors, so 1 and 3 land as 3 then 1. Delivery is
	// synchronous into the buffered inbox, so both are already queued.
	require.Equal(t, 2, len(inbox))
	require.Equal(t, []byte{3}, (<-inbox).Data)
	require.Equal(t, []byte{1}, (<-inbox).Data)
}
