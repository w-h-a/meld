package gcounter_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/gcounter"
)

// --- get, increment, value, clone ---

func TestGCounter_GetReturnsZeroForAbsent(t *testing.T) {
	// arrange
	g := gcounter.New().Increment("n1")

	// act
	value := g.Get("never-seen")

	// assert
	require.Equal(t, uint64(0), value)
}

func TestGCounter_IncrementMonotonicAndImmutable(t *testing.T) {
	// arrange
	a := gcounter.New()

	// act
	b := a.Increment("n1")
	c := b.Increment("n1")

	// assert
	require.Equal(t, uint64(0), a.Get("n1"))
	require.Equal(t, uint64(1), b.Get("n1"))
	require.Equal(t, uint64(2), c.Get("n1"))
}

func TestGCounter_ValueIsSumOfAllSlots(t *testing.T) {
	// arrange
	g := gcounter.New().
		Increment("n1").Increment("n1").
		Increment("n2").
		Increment("n3").Increment("n3").Increment("n3")

	// act
	value := g.Value()

	// assert. 2 + 1 + 3.
	require.Equal(t, uint64(6), value)
}

func TestGCounter_CloneIsEqual(t *testing.T) {
	// arrange
	og := gcounter.New().Increment("n1").Increment("n2").Increment("n1")

	// act
	clone := og.Clone()

	// assert
	require.Equal(t, uint64(2), clone.Get("n1"))
	require.Equal(t, uint64(1), clone.Get("n2"))
	require.Equal(t, og.Value(), clone.Value())
}

// --- merge ---

func TestGCounter_TwoNodesConcurrentIncrementsSurviveMerge(t *testing.T) {
	// A naive single-integer counter loses one of two concurrent
	// increments. Per-slot accounting keeps both.
	// arrange
	n1 := gcounter.New().Increment("n1")
	n2 := gcounter.New().Increment("n2")

	// act
	merged := n1.Merge(n2)

	// assert
	require.Equal(t, uint64(1), merged.Get("n1"))
	require.Equal(t, uint64(1), merged.Get("n2"))
	require.Equal(t, uint64(2), merged.Value())
}

func TestGCounter_ThreeNodesConverge_AnyMergeOrder(t *testing.T) {
	// Three nodes increment independently, then converge to the same
	// value no matter which node folds the states in which order.
	// arrange
	n1 := gcounter.New().Increment("n1").Increment("n1")                 // {n1: 2}
	n2 := gcounter.New().Increment("n2")                                 // {n2: 1}
	n3 := gcounter.New().Increment("n3").Increment("n3").Increment("n3") // {n3: 3}

	// act
	atN1 := n1.Merge(n2).Merge(n3)
	atN3 := n3.Merge(n1).Merge(n2)

	// assert
	require.Equal(t, uint64(6), atN1.Value())
	require.Equal(t, uint64(6), atN3.Value())
}

func TestGCounter_MergeKeepsHigherSlotForEachNode(t *testing.T) {
	// Merge is the element-wise maximum: each slot in the result is the
	// larger of the two inputs.
	cases := []struct {
		name        string
		a, b        gcounter.GCounter
		expectedMax map[string]uint64
	}{
		{
			"disjoint ids",
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n2"),
			map[string]uint64{"n1": 1, "n2": 1},
		},
		{
			"overlapping ids with different counts",
			gcounter.New().Increment("n1").Increment("n1").Increment("n2"),
			gcounter.New().Increment("n1"),
			map[string]uint64{"n1": 2, "n2": 1},
		},
		{
			"one input empty",
			gcounter.New(),
			gcounter.New().Increment("n1"),
			map[string]uint64{"n1": 1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			merged := c.a.Merge(c.b)

			// assert
			for id, expected := range c.expectedMax {
				require.Equal(t, expected, merged.Get(id), "slot %s", id)
			}
		})
	}
}

func TestGCounter_MergeIsCommutative(t *testing.T) {
	// Order of arguments does not matter. Both orders marshal to the
	// same canonical bytes, so they reached the same state.
	cases := []struct {
		name string
		a, b gcounter.GCounter
	}{
		{
			"disjoint ids",
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n2"),
		},
		{
			"overlapping ids with different counts",
			gcounter.New().Increment("n1").Increment("n1").Increment("n2"),
			gcounter.New().Increment("n1").Increment("n2").Increment("n2"),
		},
		{
			"one input empty",
			gcounter.New(),
			gcounter.New().Increment("n1").Increment("n2"),
		},
		{
			"both inputs empty",
			gcounter.New(),
			gcounter.New(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab, err := c.a.Merge(c.b).Marshal()
			require.NoError(t, err)
			ba, err := c.b.Merge(c.a).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, ab, ba)
		})
	}
}

func TestGCounter_MergeIsAssociative(t *testing.T) {
	// Grouping does not matter.
	cases := []struct {
		name    string
		a, b, c gcounter.GCounter
	}{
		{
			"disjoint ids across all three",
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n2"),
			gcounter.New().Increment("n3"),
		},
		{
			"overlapping ids, each higher on a different one",
			gcounter.New().Increment("n1").Increment("n1"),
			gcounter.New().Increment("n1").Increment("n2"),
			gcounter.New().Increment("n2").Increment("n3"),
		},
		{
			"one input empty",
			gcounter.New(),
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n2"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left, err := c.a.Merge(c.b).Merge(c.c).Marshal()
			require.NoError(t, err)
			right, err := c.a.Merge(c.b.Merge(c.c)).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, left, right)
		})
	}
}

func TestGCounter_MergeIsIdempotent(t *testing.T) {
	// Merging the same value twice has the same effect as merging it
	// once. So a duplicated or replayed message is harmless.
	cases := []struct {
		name string
		a, b gcounter.GCounter
	}{
		{
			"merging an empty value",
			gcounter.New().Increment("n1").Increment("n2"),
			gcounter.New(),
		},
		{
			"merging a disjoint id",
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n2"),
		},
		{
			"merging a strictly higher value",
			gcounter.New().Increment("n1"),
			gcounter.New().Increment("n1").Increment("n1"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			onceBytes, err := once.Marshal()
			require.NoError(t, err)
			twiceBytes, err := once.Merge(c.b).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, onceBytes, twiceBytes)
		})
	}
}

// --- marshal / unmarshal ---

func TestGCounter_MarshalUnmarshalRoundTrip(t *testing.T) {
	// arrange
	original := gcounter.New().Increment("n1").Increment("n2").Increment("n1")

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded gcounter.GCounter
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, original.Get("n1"), decoded.Get("n1"))
	require.Equal(t, original.Get("n2"), decoded.Get("n2"))
	require.Equal(t, original.Value(), decoded.Value())
}

func TestGCounter_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := gcounter.New()

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded gcounter.GCounter
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, uint64(0), decoded.Value())
}

func TestGCounter_MarshalIsCanonical(t *testing.T) {
	// Two counters that reached the same state by different increment
	// orders marshal to identical bytes. So the anti-entropy layer can
	// compare or hash wire bytes to detect divergence.
	// arrange
	a := gcounter.New().Increment("n2").Increment("n1").Increment("n2")
	b := gcounter.New().Increment("n2").Increment("n2").Increment("n1")

	// act
	ab, err := a.Marshal()
	require.NoError(t, err)
	bb, err := b.Marshal()
	require.NoError(t, err)

	// assert
	require.Equal(t, ab, bb)
}

func TestGCounter_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(nil)

	// assert
	require.Error(t, err)
}

func TestGCounter_UnmarshalRejectsTruncatedIDBytes(t *testing.T) {
	// arrange. count=1, idLen=10, no id payload.
	bytes := []byte{0x01, 0x0a}
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestGCounter_UnmarshalRejectsZeroValuedSlot(t *testing.T) {
	// arrange. count=1, idLen=2, id="n1", value=0. A zero slot is
	// indistinguishable from an absent one, so it has no canonical form.
	bytes := []byte{0x01, 0x02, 'n', '1', 0x00}
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestGCounter_UnmarshalRejectsUnsortedSlots(t *testing.T) {
	// arrange. count=2, (n2,1) then (n1,1). Out of order.
	bytes := []byte{
		0x02,
		0x02, 'n', '2', 0x01,
		0x02, 'n', '1', 0x01,
	}
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestGCounter_UnmarshalRejectsDuplicateSlots(t *testing.T) {
	// arrange. count=2, (n1,1) then (n1,2). Equal id violates "strictly
	// increasing".
	bytes := []byte{
		0x02,
		0x02, 'n', '1', 0x01,
		0x02, 'n', '1', 0x02,
	}
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestGCounter_UnmarshalRejectsHugeCount(t *testing.T) {
	// arrange. count = 2^57 (huge varint), then empty body.
	bytes := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	var g gcounter.GCounter

	// act
	err := g.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}
