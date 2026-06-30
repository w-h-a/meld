package memory_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/store/memory"
)

func TestMemory_Lifecycle(t *testing.T) {
	// arrange
	s, err := memory.New()
	require.NoError(t, err)

	ctx := context.Background()

	// act: load before any save
	data, found, err := s.Load(ctx)

	// assert
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)

	// act: save, then load
	require.NoError(t, s.Save(ctx, []byte("first")))
	data, found, err = s.Load(ctx)

	// assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("first"), data)

	// act: save again, then load
	require.NoError(t, s.Save(ctx, []byte("second")))
	data, found, err = s.Load(ctx)

	// assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("second"), data)
}
