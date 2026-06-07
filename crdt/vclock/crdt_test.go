package vclock_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/vclock"
)

// --- get, increment, and clone ---

func TestVersionVector_GetReturnsZeroForAbsent(t *testing.T) {
	// arrange
	v := vclock.New().Increment("n1")

	// act
	value := v.Get("never-seen")

	// assert
	require.Equal(t, uint64(0), value)
}

func TestVersionVector_IncrementMonotonicAndImmutability(t *testing.T) {
	// arrange
	a := vclock.New()

	// act
	b := a.Increment("n1")
	c := b.Increment("n1")

	// assert
	require.Equal(t, uint64(0), a.Get("n1"))
	require.Equal(t, uint64(1), b.Get("n1"))
	require.Equal(t, uint64(2), c.Get("n1"))
	require.Equal(t, vclock.Greater, c.Compare(b))
	require.Equal(t, vclock.Greater, b.Compare(a))
}

func TestVersionVector_CloneIsEqual(t *testing.T) {
	// arrange
	og := vclock.New().Increment("n1").Increment("n2")
	clone := og.Clone()

	// act + assert
	require.Equal(t, vclock.Equal, og.Compare(clone))
}

// --- compare causal ordering ---

func TestVersionVector_EqualWhenAllCountersMatch(t *testing.T) {
	// arrange
	a := vclock.New().Increment("n1").Increment("n2").Increment("n1")
	b := vclock.New().Increment("n2").Increment("n1").Increment("n1")

	// act + assert
	require.Equal(t, vclock.Equal, a.Compare(b))
	require.Equal(t, vclock.Equal, b.Compare(a))
}

func TestVersionVector_LaterDominatesEarlier(t *testing.T) {
	// arrange
	earlier := vclock.New().Increment("n1")
	later := earlier.Increment("n1").Increment("n2")

	// act
	forward := later.Compare(earlier)
	reverse := earlier.Compare(later)

	// assert
	require.Equal(t, vclock.Greater, forward)
	require.Equal(t, vclock.Lesser, reverse)
}

func TestVersionVector_NeitherDominatesWhenConcurrent(t *testing.T) {
	// arrange
	a := vclock.New().Increment("n1")
	b := vclock.New().Increment("n2")
	concurrent := []vclock.Ordering{vclock.ConcurrentGreater, vclock.ConcurrentLesser}

	// act + assert
	require.Contains(t, concurrent, a.Compare(b))
	require.Contains(t, concurrent, b.Compare(a))
}

func TestVersionVector_AntisymmetricWhenConcurrent(t *testing.T) {
	// arrange
	a := vclock.New().Increment("n1").Increment("n2").Increment("n2").Increment("n2")
	b := vclock.New().Increment("n1").Increment("n1").Increment("n1").Increment("n2")

	// act
	ab := a.Compare(b)
	ba := b.Compare(a)

	// assert
	require.NotEqual(t, ab, ba)
}

// --- merge ---

func TestVersionVector_MergeKeepsDominator(t *testing.T) {
	// arrange
	earlier := vclock.New().Increment("n1")
	later := earlier.Increment("n2")

	// act
	merged := earlier.Merge(later)

	// assert
	require.Equal(t, vclock.Equal, merged.Compare(later))
}

func TestVersionVector_MergeConcurrentRecordsBoth(t *testing.T) {
	// arrange
	a := vclock.New().Increment("n1")
	b := vclock.New().Increment("n2")

	// act
	merged := a.Merge(b)

	// assert
	require.Equal(t, uint64(1), merged.Get("n1"))
	require.Equal(t, uint64(1), merged.Get("n2"))
	require.Equal(t, vclock.Greater, merged.Compare(a))
	require.Equal(t, vclock.Greater, merged.Compare(b))
}

func TestVersionVector_MergeIsCommutative(t *testing.T) {
	// Order of arguments does not matter.
	cases := []struct {
		name string
		a    vclock.VectorClock
		b    vclock.VectorClock
	}{
		{
			"disjoint ids",
			vclock.New().Increment("n1"),
			vclock.New().Increment("n2"),
		},
		{
			"overlapping ids with different counters",
			vclock.New().Increment("n1").Increment("n1").Increment("n2"),
			vclock.New().Increment("n1").Increment("n2").Increment("n2"),
		},
		{
			"one input empty",
			vclock.New(),
			vclock.New().Increment("n1").Increment("n2"),
		},
		{
			"both inputs empty",
			vclock.New(),
			vclock.New(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab := c.a.Merge(c.b)
			ba := c.b.Merge(c.a)

			// assert
			require.Equal(t, vclock.Equal, ab.Compare(ba))
		})
	}
}

func TestVersionVector_MergeIsAssociative(t *testing.T) {
	// Grouping does not matter.
	cases := []struct {
		name    string
		a, b, c vclock.VectorClock
	}{
		{
			"disjoint ids across all three",
			vclock.New().Increment("n1"),
			vclock.New().Increment("n2"),
			vclock.New().Increment("n3"),
		},
		{
			"overlapping ids, each side higher on a different one",
			vclock.New().Increment("n1").Increment("n1"),
			vclock.New().Increment("n1").Increment("n2"),
			vclock.New().Increment("n2").Increment("n3"),
		},
		{
			"one input empty",
			vclock.New(),
			vclock.New().Increment("n1"),
			vclock.New().Increment("n2"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left := c.a.Merge(c.b).Merge(c.c)
			right := c.a.Merge(c.b.Merge(c.c))

			// assert
			require.Equal(t, vclock.Equal, left.Compare(right))
		})
	}
}

func TestVersionVector_MergeIsIdempotent(t *testing.T) {
	// Merging the same value twice has the same effect as merging it
	// once.
	cases := []struct {
		name string
		a, b vclock.VectorClock
	}{
		{
			"merging an empty value",
			vclock.New().Increment("n1").Increment("n2"),
			vclock.New(),
		},
		{
			"merging a disjoint id",
			vclock.New().Increment("n1"),
			vclock.New().Increment("n2"),
		},
		{
			"merging a strictly later value",
			vclock.New().Increment("n1"),
			vclock.New().Increment("n1").Increment("n1"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			twice := once.Merge(c.b)

			// assert
			require.Equal(t, vclock.Equal, once.Compare(twice))
		})
	}
}

func TestVersionVector_MergeNeverLosesInformationFromEitherInput(t *testing.T) {
	cases := []struct {
		name        string
		a, b        vclock.VectorClock
		expectedMax map[string]uint64
	}{
		{
			"disjoint ids",
			vclock.New().Increment("n1"),
			vclock.New().Increment("n2"),
			map[string]uint64{"n1": 1, "n2": 1},
		},
		{
			"overlapping ids with different counters",
			vclock.New().Increment("n1").Increment("n2"),
			vclock.New().Increment("n1").Increment("n1"),
			map[string]uint64{"n1": 2, "n2": 1},
		},
		{
			"one input empty",
			vclock.New(),
			vclock.New().Increment("n1"),
			map[string]uint64{"n1": 1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			m := c.a.Merge(c.b)

			// assert
			for id, expected := range c.expectedMax {
				require.Equal(t, expected, m.Get(id), "id %s", id)
			}
		})
	}
}

// --- marshal/unmarshal ---

func TestVersionVector_MarshalUnmarshalRoundTrip(t *testing.T) {
	// arrange
	original := vclock.New().Increment("n1").Increment("n2").Increment("n1")

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded vclock.VectorClock
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, vclock.Equal, original.Compare(decoded))
	require.Equal(t, original.Get("n1"), decoded.Get("n1"))
	require.Equal(t, original.Get("n2"), decoded.Get("n2"))
}

func TestVersionVector_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := vclock.New()

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded vclock.VectorClock
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, vclock.Equal, original.Compare(decoded))
}

func TestVersionVector_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(nil)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsTruncatedIDBytes(t *testing.T) {
	// arrange. count=1, idLen=10, no id payload.
	bytes := []byte{0x01, 0x0a}
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsZeroValuedEntry(t *testing.T) {
	// arrange. count=1, idLen=2, id="n1", value=0.
	bytes := []byte{0x01, 0x02, 'n', '1', 0x00}
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsUnsortedEntries(t *testing.T) {
	// arrange. count=2, (n2,1) then (n1,1). Out of order.
	bytes := []byte{
		0x02,
		0x02, 'n', '2', 0x01,
		0x02, 'n', '1', 0x01,
	}
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsDuplicateEntries(t *testing.T) {
	// arrange. count=2, (n1,1) then (n1,2). Equal id violates "strictly increasing".
	bytes := []byte{
		0x02,
		0x02, 'n', '1', 0x01,
		0x02, 'n', '1', 0x02,
	}
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsHugeCount(t *testing.T) {
	// arrange. count = 2^57 (huge varint), then empty body.
	bytes := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	var v vclock.VectorClock

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}
