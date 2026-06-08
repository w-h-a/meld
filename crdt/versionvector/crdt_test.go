package versionvector_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/versionvector"
)

// --- get, increment, and clone ---

func TestVersionVector_GetReturnsZeroForAbsent(t *testing.T) {
	// arrange
	v := versionvector.New().Increment("n1")

	// act
	value := v.Get("never-seen")

	// assert
	require.Equal(t, uint64(0), value)
}

func TestVersionVector_IncrementMonotonicAndImmutability(t *testing.T) {
	// arrange
	a := versionvector.New()

	// act
	b := a.Increment("n1")
	c := b.Increment("n1")

	// assert
	require.Equal(t, uint64(0), a.Get("n1"))
	require.Equal(t, uint64(1), b.Get("n1"))
	require.Equal(t, uint64(2), c.Get("n1"))
	require.Equal(t, versionvector.Greater, c.Compare(b))
	require.Equal(t, versionvector.Greater, b.Compare(a))
}

func TestVersionVector_CloneIsEqual(t *testing.T) {
	// arrange
	og := versionvector.New().Increment("n1").Increment("n2")
	clone := og.Clone()

	// act + assert
	require.Equal(t, versionvector.Equal, og.Compare(clone))
}

// --- compare causal ordering ---

func TestVersionVector_EqualWhenAllCountersMatch(t *testing.T) {
	// arrange
	a := versionvector.New().Increment("n1").Increment("n2").Increment("n1")
	b := versionvector.New().Increment("n2").Increment("n1").Increment("n1")

	// act + assert
	require.Equal(t, versionvector.Equal, a.Compare(b))
	require.Equal(t, versionvector.Equal, b.Compare(a))
}

func TestVersionVector_LaterDominatesEarlier(t *testing.T) {
	// arrange
	earlier := versionvector.New().Increment("n1")
	later := earlier.Increment("n1").Increment("n2")

	// act
	forward := later.Compare(earlier)
	reverse := earlier.Compare(later)

	// assert
	require.Equal(t, versionvector.Greater, forward)
	require.Equal(t, versionvector.Lesser, reverse)
}

func TestVersionVector_NeitherDominatesWhenConcurrent(t *testing.T) {
	// arrange
	a := versionvector.New().Increment("n1")
	b := versionvector.New().Increment("n2")
	concurrent := []versionvector.Ordering{versionvector.ConcurrentGreater, versionvector.ConcurrentLesser}

	// act + assert
	require.Contains(t, concurrent, a.Compare(b))
	require.Contains(t, concurrent, b.Compare(a))
}

func TestVersionVector_AntisymmetricWhenConcurrent(t *testing.T) {
	// arrange
	a := versionvector.New().Increment("n1").Increment("n2").Increment("n2").Increment("n2")
	b := versionvector.New().Increment("n1").Increment("n1").Increment("n1").Increment("n2")

	// act
	ab := a.Compare(b)
	ba := b.Compare(a)

	// assert
	require.NotEqual(t, ab, ba)
}

// --- merge ---

func TestVersionVector_MergeKeepsDominator(t *testing.T) {
	// arrange
	earlier := versionvector.New().Increment("n1")
	later := earlier.Increment("n2")

	// act
	merged := earlier.Merge(later)

	// assert
	require.Equal(t, versionvector.Equal, merged.Compare(later))
}

func TestVersionVector_MergeConcurrentRecordsBoth(t *testing.T) {
	// arrange
	a := versionvector.New().Increment("n1")
	b := versionvector.New().Increment("n2")

	// act
	merged := a.Merge(b)

	// assert
	require.Equal(t, uint64(1), merged.Get("n1"))
	require.Equal(t, uint64(1), merged.Get("n2"))
	require.Equal(t, versionvector.Greater, merged.Compare(a))
	require.Equal(t, versionvector.Greater, merged.Compare(b))
}

func TestVersionVector_MergeIsCommutative(t *testing.T) {
	// Order of arguments does not matter.
	cases := []struct {
		name string
		a    versionvector.VersionVector
		b    versionvector.VersionVector
	}{
		{
			"disjoint ids",
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n2"),
		},
		{
			"overlapping ids with different counters",
			versionvector.New().Increment("n1").Increment("n1").Increment("n2"),
			versionvector.New().Increment("n1").Increment("n2").Increment("n2"),
		},
		{
			"one input empty",
			versionvector.New(),
			versionvector.New().Increment("n1").Increment("n2"),
		},
		{
			"both inputs empty",
			versionvector.New(),
			versionvector.New(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab := c.a.Merge(c.b)
			ba := c.b.Merge(c.a)

			// assert
			require.Equal(t, versionvector.Equal, ab.Compare(ba))
		})
	}
}

func TestVersionVector_MergeIsAssociative(t *testing.T) {
	// Grouping does not matter.
	cases := []struct {
		name    string
		a, b, c versionvector.VersionVector
	}{
		{
			"disjoint ids across all three",
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n2"),
			versionvector.New().Increment("n3"),
		},
		{
			"overlapping ids, each side higher on a different one",
			versionvector.New().Increment("n1").Increment("n1"),
			versionvector.New().Increment("n1").Increment("n2"),
			versionvector.New().Increment("n2").Increment("n3"),
		},
		{
			"one input empty",
			versionvector.New(),
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n2"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left := c.a.Merge(c.b).Merge(c.c)
			right := c.a.Merge(c.b.Merge(c.c))

			// assert
			require.Equal(t, versionvector.Equal, left.Compare(right))
		})
	}
}

func TestVersionVector_MergeIsIdempotent(t *testing.T) {
	// Merging the same value twice has the same effect as merging it
	// once.
	cases := []struct {
		name string
		a, b versionvector.VersionVector
	}{
		{
			"merging an empty value",
			versionvector.New().Increment("n1").Increment("n2"),
			versionvector.New(),
		},
		{
			"merging a disjoint id",
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n2"),
		},
		{
			"merging a strictly later value",
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n1").Increment("n1"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			twice := once.Merge(c.b)

			// assert
			require.Equal(t, versionvector.Equal, once.Compare(twice))
		})
	}
}

func TestVersionVector_MergeNeverLosesInformationFromEitherInput(t *testing.T) {
	cases := []struct {
		name        string
		a, b        versionvector.VersionVector
		expectedMax map[string]uint64
	}{
		{
			"disjoint ids",
			versionvector.New().Increment("n1"),
			versionvector.New().Increment("n2"),
			map[string]uint64{"n1": 1, "n2": 1},
		},
		{
			"overlapping ids with different counters",
			versionvector.New().Increment("n1").Increment("n2"),
			versionvector.New().Increment("n1").Increment("n1"),
			map[string]uint64{"n1": 2, "n2": 1},
		},
		{
			"one input empty",
			versionvector.New(),
			versionvector.New().Increment("n1"),
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

// --- marshal / unmarshal ---

func TestVersionVector_MarshalUnmarshalRoundTrip(t *testing.T) {
	// arrange
	original := versionvector.New().Increment("n1").Increment("n2").Increment("n1")

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded versionvector.VersionVector
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, versionvector.Equal, original.Compare(decoded))
	require.Equal(t, original.Get("n1"), decoded.Get("n1"))
	require.Equal(t, original.Get("n2"), decoded.Get("n2"))
}

func TestVersionVector_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := versionvector.New()

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded versionvector.VersionVector
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.Equal(t, versionvector.Equal, original.Compare(decoded))
}

func TestVersionVector_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var v versionvector.VersionVector

	// act
	err := v.Unmarshal(nil)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsTruncatedIDBytes(t *testing.T) {
	// arrange. count=1, idLen=10, no id payload.
	bytes := []byte{0x01, 0x0a}
	var v versionvector.VersionVector

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsZeroValuedEntry(t *testing.T) {
	// arrange. count=1, idLen=2, id="n1", value=0.
	bytes := []byte{0x01, 0x02, 'n', '1', 0x00}
	var v versionvector.VersionVector

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
	var v versionvector.VersionVector

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
	var v versionvector.VersionVector

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestVersionVector_UnmarshalRejectsHugeCount(t *testing.T) {
	// arrange. count = 2^57 (huge varint), then empty body.
	bytes := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	var v versionvector.VersionVector

	// act
	err := v.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}
