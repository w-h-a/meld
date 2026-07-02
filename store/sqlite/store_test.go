package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/store"
	"github.com/w-h-a/meld/store/sqlite"
)

func TestSqlite_DurableAcrossReopen(t *testing.T) {
	// arrange
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()

	s1, err := sqlite.New(store.WithLocation(path))
	require.NoError(t, err)

	// act: load before any save
	data, found, err := s1.Load(ctx)

	// assert
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)

	// act+assert: save, then close the first handle to force a real reopen
	require.NoError(t, s1.Save(ctx, []byte("first")))
	require.NoError(t, s1.Close(ctx))

	// arrange
	s2, err := sqlite.New(store.WithLocation(path))
	require.NoError(t, err)

	// act
	data, found, err = s2.Load(ctx)

	// assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("first"), data)

	// act: a second save fully replaces the prior blob
	require.NoError(t, s2.Save(ctx, []byte("second")))
	data, found, err = s2.Load(ctx)

	// assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("second"), data)

	// act+assert
	require.NoError(t, s2.Close(ctx))
}
