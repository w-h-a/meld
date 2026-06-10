package pncounter_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/pncounter"
)

// --- construction, accessors, immutability ---

func TestPNCounter_NewIsZero(t *testing.T) {
	// arrange + act
	pn := pncounter.New()

	// assert
	require.Equal(t, int64(0), pn.Value())
}

func TestPNCounter_IncrementReturnsNewCounterWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := pncounter.New()

	// act
	b := a.Increment("n1")

	// assert
	require.Equal(t, int64(0), a.Value())
	require.Equal(t, int64(1), b.Value())
}

func TestPNCounter_DecrementReturnsNewCounterWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := pncounter.New()

	// act
	b := a.Decrement("n1")

	// assert
	require.Equal(t, int64(0), a.Value())
	require.Equal(t, int64(-1), b.Value())
}

func TestPNCounter_CloneIsIndependent(t *testing.T) {
	// arrange
	a := pncounter.New().Increment("n1").Increment("n1") // value 2

	// act
	b := a.Clone()
	b = b.Decrement("n1")

	// assert
	require.Equal(t, int64(2), a.Value())
	require.Equal(t, int64(1), b.Value())
}

// --- value: increments minus decrements ---

func TestPNCounter_ValueIsIncrementsMinusDecrements(t *testing.T) {
	// arrange
	pn := pncounter.New().
		Increment("n1").Increment("n1").Increment("n2"). // P = 3
		Decrement("n1")                                  // N = 1

	// act
	value := pn.Value()

	// assert
	require.Equal(t, int64(2), value)
}

func TestPNCounter_ValueCanGoNegative_WhenDecrementsExceedIncrements(t *testing.T) {
	// arrange
	pn := pncounter.New().Increment("n1").Decrement("n1").Decrement("n1")

	// act
	value := pn.Value()

	// assert
	require.Equal(t, int64(-1), value)
}

// --- convergence ---

func TestPNCounter_OpenConnectionsAcrossThreeNodes(t *testing.T) {
	// arrange. Three nodes track a shared count of open connections.
	// Each opens a connection (increment) and closes one (decrement)
	// locally.
	n1 := pncounter.New().Increment("n1").Increment("n1").Decrement("n1") // net +1
	n2 := pncounter.New().Increment("n2").Increment("n2").Increment("n2") // net +3
	n3 := pncounter.New().Increment("n3").Decrement("n3")                 // net 0

	// act
	converged := n1.Merge(n2).Merge(n3)

	// assert
	require.Equal(t, int64(4), converged.Value())
}

func TestPNCounter_ConvergenceUnderConcurrentIncrementAndDecrement(t *testing.T) {
	// arrange. Two replicas concurrently increment and decrement.
	n1 := pncounter.New().Increment("n1").Increment("n1") // P={n1:2}
	n2 := pncounter.New().Decrement("n2").Increment("n2") // P={n2:1}, N={n2:1}

	// act
	ab := n1.Merge(n2)
	ba := n2.Merge(n1)

	// assert
	require.Equal(t, int64(2), ab.Value())
	require.Equal(t, ab.Value(), ba.Value())
}

// --- crdt properties: commutative, associative, idempotent ---

func TestPNCounter_MergeIsCommutative(t *testing.T) {
	// Order of arguments does not matter.
	cases := []struct {
		name string
		a, b pncounter.PNCounter
	}{
		{
			"disjoint nodes, mixed increment and decrement",
			pncounter.New().Increment("n1").Decrement("n1"),
			pncounter.New().Increment("n2").Increment("n2"),
		},
		{
			"overlapping nodes",
			pncounter.New().Increment("n1").Increment("n1").Decrement("n2"),
			pncounter.New().Increment("n1").Decrement("n2").Decrement("n2"),
		},
		{
			"one side empty",
			pncounter.New(),
			pncounter.New().Increment("n1").Decrement("n2"),
		},
		{
			"both sides empty",
			pncounter.New(),
			pncounter.New(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab := c.a.Merge(c.b)
			ba := c.b.Merge(c.a)

			// assert
			require.Equal(t, ab.Value(), ba.Value())
		})
	}
}

func TestPNCounter_MergeIsAssociative(t *testing.T) {
	// Grouping does not matter.
	cases := []struct {
		name    string
		a, b, c pncounter.PNCounter
	}{
		{
			"disjoint nodes across all three",
			pncounter.New().Increment("n1"),
			pncounter.New().Decrement("n2"),
			pncounter.New().Increment("n3").Decrement("n3"),
		},
		{
			"overlapping nodes, each higher on a different slot",
			pncounter.New().Increment("n1").Increment("n1").Decrement("n2"),
			pncounter.New().Increment("n1").Decrement("n2").Decrement("n2"),
			pncounter.New().Decrement("n2").Increment("n3"),
		},
		{
			"one input empty",
			pncounter.New(),
			pncounter.New().Increment("n1"),
			pncounter.New().Decrement("n2"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left := c.a.Merge(c.b).Merge(c.c)
			right := c.a.Merge(c.b.Merge(c.c))

			// assert
			require.Equal(t, left.Value(), right.Value())
		})
	}
}

func TestPNCounter_MergeIsIdempotent(t *testing.T) {
	// Merging the same value twice has the same effect as merging it
	// once.
	cases := []struct {
		name string
		a, b pncounter.PNCounter
	}{
		{
			"merging an empty counter",
			pncounter.New().Increment("n1").Decrement("n2"),
			pncounter.New(),
		},
		{
			"merging a concurrent counter",
			pncounter.New().Increment("n1"),
			pncounter.New().Decrement("n2"),
		},
		{
			"merging a strictly higher counter",
			pncounter.New().Increment("n1").Decrement("n1"),
			pncounter.New().Increment("n1").Increment("n1").Decrement("n1"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			twice := once.Merge(c.b)

			// assert
			require.Equal(t, once.Value(), twice.Value())
		})
	}
}

// --- marshal / unmarshal ---

func TestPNCounter_MarshalUnmarshalRoundTrip(t *testing.T) {
	// arrange. A negative reading exercises both P and N.
	original := pncounter.New().
		Increment("n1").Increment("n1").
		Decrement("n1").Decrement("n2").Decrement("n2")

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded pncounter.PNCounter
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert. 2 - (1 + 2).
	require.Equal(t, int64(-1), decoded.Value())
	require.Equal(t, original.Value(), decoded.Value())
}

func TestPNCounter_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := pncounter.New()

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded pncounter.PNCounter
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, int64(0), decoded.Value())
}

func TestPNCounter_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var pn pncounter.PNCounter

	// act
	err := pn.Unmarshal(nil)

	// assert
	require.Error(t, err)
}

func TestPNCounter_UnmarshalRejectsTruncatedP(t *testing.T) {
	// arrange. The P length header claims 10 bytes, none follow.
	bytes := []byte{0x0a}
	var pn pncounter.PNCounter

	// act
	err := pn.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestPNCounter_UnmarshalSurfacesInnerCounterError(t *testing.T) {
	// arrange. pLen=5 frames a P body of count=1, idLen=2, id="n1",
	// value=0. The inner G-Counter rejects the zero-valued slot, and
	// that error surfaces here.
	bytes := []byte{
		0x05,                       // len(pBytes) = 5
		0x01, 0x02, 'n', '1', 0x00, // P: count=1, slot (n1, value=0)
	}
	var pn pncounter.PNCounter

	// act
	err := pn.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}
