package causalcontext_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/causalcontext"
	"github.com/w-h-a/meld/crdt/versionvector"
)

// --- contains ---

func TestCausalContext_ContainsSeenAndUnseenDots(t *testing.T) {
	// A context that has seen n1's 1, 2, and 4, but not 3. So 1 and 2 sit in
	// normals, 4 is an exception, and 3 is the gap.
	// arrange
	c := causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 4)

	cases := []struct {
		name    string
		node    string
		counter uint64
		want    bool
	}{
		{"first dot in normals", "n1", 1, true},
		{"last dot in normals", "n1", 2, true},
		{"dot in exceptions", "n1", 4, true},
		{"dot in the gap", "n1", 3, false},
		{"dot above everything seen", "n1", 5, false},
		{"dot for an absent node", "n2", 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			got := c.Contains(tc.node, tc.counter)

			// assert
			require.Equal(t, tc.want, got)
		})
	}
}

// --- observe ---

func TestCausalContext_ObserveOutOfOrderHoldsAsException(t *testing.T) {
	// Seeing 4 before 3 records 4 but leaves the gap at 3 open. If 4 had been
	// pulled into the in-order count, 3 would read as seen too. It does not.
	// arrange
	c := causalcontext.New().Observe("n1", 1).Observe("n1", 2)

	// act
	c = c.Observe("n1", 4)

	// assert
	require.True(t, c.Contains("n1", 4))
	require.False(t, c.Contains("n1", 3))
}

func TestCausalContext_ObserveFillingGapCollapsesExceptions(t *testing.T) {
	// Seeing 4, 5, 6 while 3 is missing holds them as exceptions. Then seeing 3
	// fills the gap and folds 4, 5, 6 back into the count, so the result is
	// identical to having seen 1 through 6 in order.
	// arrange
	outOfOrder := causalcontext.New().
		Observe("n1", 1).Observe("n1", 2).
		Observe("n1", 4).Observe("n1", 5).Observe("n1", 6)
	inOrder := causalcontext.New().
		Observe("n1", 1).Observe("n1", 2).Observe("n1", 3).
		Observe("n1", 4).Observe("n1", 5).Observe("n1", 6)

	// act
	filled := outOfOrder.Observe("n1", 3)

	// assert
	filledBytes, err := filled.Marshal()
	require.NoError(t, err)
	inOrderBytes, err := inOrder.Marshal()
	require.NoError(t, err)
	require.Equal(t, inOrderBytes, filledBytes)
}

func TestCausalContext_ObserveAlreadySeenIsNoOp(t *testing.T) {
	// Observing a dot already in the count changes nothing.
	// arrange
	c := causalcontext.New().Observe("n1", 1).Observe("n1", 2)
	before, err := c.Marshal()
	require.NoError(t, err)

	// act
	again := c.Observe("n1", 1)

	// assert
	after, err := again.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// --- equal ---

func TestCausalContext_EqualIsTrueIffSameDots(t *testing.T) {
	cases := []struct {
		name     string
		a, b     causalcontext.CausalContext
		expected bool
	}{
		{
			"same dots, contiguous either way",
			causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 3),
			causalcontext.New().Observe("n1", 3).Observe("n1", 1).Observe("n1", 2),
			true,
		},
		{
			"same exception set, different observe order",
			causalcontext.New().Observe("n1", 3).Observe("n1", 5),
			causalcontext.New().Observe("n1", 5).Observe("n1", 3),
			true,
		},
		{
			"different dot sets",
			causalcontext.New().Observe("n1", 1),
			causalcontext.New().Observe("n2", 1),
			false,
		},
		{
			"one has an extra exception",
			causalcontext.New().Observe("n1", 1).Observe("n1", 3),
			causalcontext.New().Observe("n1", 1).Observe("n1", 3).Observe("n1", 5),
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.True(t, c.a.Equal(c.a))
			require.Equal(t, c.expected, c.a.Equal(c.b))
			require.Equal(t, c.expected, c.b.Equal(c.a))
		})
	}
}

// --- merge ---

func TestCausalContext_MergeObservesUnionAndCollapses(t *testing.T) {
	// c saw n1's 1, 2, 4 (4 held as an exception). other saw n1's 1, 2, 3.
	// Merging takes the union, and other's 3 fills c's gap so 4 folds in. The
	// result is the same as having seen 1, 2, 3, 4 in order.
	// arrange
	c := causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 4)
	other := causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 3)

	// act
	merged := c.Merge(other)

	// assert
	mergedBytes, err := merged.Marshal()
	require.NoError(t, err)
	inOrder, err := causalcontext.New().
		Observe("n1", 1).Observe("n1", 2).Observe("n1", 3).Observe("n1", 4).
		Marshal()
	require.NoError(t, err)
	require.Equal(t, inOrder, mergedBytes)
}

func TestCausalContext_MergeIsCommutative(t *testing.T) {
	cases := []struct {
		name string
		a, b causalcontext.CausalContext
	}{
		{
			"disjoint nodes",
			causalcontext.New().Observe("n1", 1),
			causalcontext.New().Observe("n2", 1),
		},
		{
			"overlap where one side fills the other's gap",
			causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 4),
			causalcontext.New().Observe("n1", 1).Observe("n1", 3),
		},
		{
			"one side empty",
			causalcontext.New(),
			causalcontext.New().Observe("n1", 1).Observe("n1", 3),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			ab, err := tc.a.Merge(tc.b).Marshal()
			require.NoError(t, err)
			ba, err := tc.b.Merge(tc.a).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, ab, ba)
		})
	}
}

func TestCausalContext_MergeIsAssociative(t *testing.T) {
	cases := []struct {
		name    string
		a, b, c causalcontext.CausalContext
	}{
		{
			"disjoint nodes",
			causalcontext.New().Observe("n1", 1),
			causalcontext.New().Observe("n2", 1),
			causalcontext.New().Observe("n3", 1),
		},
		{
			"overlapping with exceptions across all three",
			causalcontext.New().Observe("n1", 1).Observe("n1", 4),
			causalcontext.New().Observe("n1", 1).Observe("n1", 2),
			causalcontext.New().Observe("n1", 3).Observe("n2", 1),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			left, err := tc.a.Merge(tc.b).Merge(tc.c).Marshal()
			require.NoError(t, err)
			right, err := tc.a.Merge(tc.b.Merge(tc.c)).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, left, right)
		})
	}
}

func TestCausalContext_MergeIsIdempotent(t *testing.T) {
	cases := []struct {
		name string
		a, b causalcontext.CausalContext
	}{
		{
			"merging an empty context",
			causalcontext.New().Observe("n1", 1).Observe("n1", 3),
			causalcontext.New(),
		},
		{
			"merging a disjoint node",
			causalcontext.New().Observe("n1", 1),
			causalcontext.New().Observe("n2", 1),
		},
		{
			"merging an overlapping context with an exception",
			causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n1", 4),
			causalcontext.New().Observe("n1", 1).Observe("n1", 3),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			once := tc.a.Merge(tc.b)
			onceBytes, err := once.Marshal()
			require.NoError(t, err)
			twiceBytes, err := once.Merge(tc.b).Marshal()
			require.NoError(t, err)

			// assert
			require.Equal(t, onceBytes, twiceBytes)
		})
	}
}

// --- marshal / unmarshal ---

func TestCausalContext_MarshalUnmarshalRoundTripWithExceptions(t *testing.T) {
	// arrange
	original := causalcontext.New().
		Observe("n1", 1).Observe("n1", 2).Observe("n1", 4).
		Observe("n2", 1)

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded causalcontext.CausalContext
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.True(t, decoded.Contains("n1", 2))
	require.True(t, decoded.Contains("n1", 4))
	require.False(t, decoded.Contains("n1", 3))
	require.True(t, decoded.Contains("n2", 1))
}

func TestCausalContext_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := causalcontext.New()

	// act
	bytes, err := original.Marshal()
	require.NoError(t, err)
	var decoded causalcontext.CausalContext
	require.NoError(t, decoded.Unmarshal(bytes))

	// assert
	require.False(t, decoded.Contains("n1", 1))
}

func TestCausalContext_MarshalIsCanonical(t *testing.T) {
	// Two contexts that reached the same dots by different observe orders marshal
	// to identical bytes, because Marshal sorts the exceptions.
	// arrange
	a := causalcontext.New().Observe("n1", 4).Observe("n2", 1).Observe("n1", 5)
	b := causalcontext.New().Observe("n2", 1).Observe("n1", 5).Observe("n1", 4)

	// act
	ab, err := a.Marshal()
	require.NoError(t, err)
	bb, err := b.Marshal()
	require.NoError(t, err)

	// assert
	require.Equal(t, ab, bb)
}

func TestCausalContext_MarshalExceptionFreeIsConstantOverVersionVector(t *testing.T) {
	// With no exceptions, the context costs the version vector bytes plus a fixed
	// two: a length prefix and a zero exception count. Not growth with the vector.
	// arrange
	c := causalcontext.New().Observe("n1", 1).Observe("n1", 2).Observe("n2", 1)
	vv := versionvector.New().Increment("n1").Increment("n1").Increment("n2")

	// act
	cBytes, err := c.Marshal()
	require.NoError(t, err)
	vvBytes, err := vv.Marshal()
	require.NoError(t, err)

	// assert
	require.Equal(t, len(vvBytes)+2, len(cBytes))
}

func TestCausalContext_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var c causalcontext.CausalContext

	// act
	err := c.Unmarshal(nil)

	// assert
	require.Error(t, err)
}

func TestCausalContext_UnmarshalRejectsTruncatedNormals(t *testing.T) {
	// arrange. normals length says 5, but no normals bytes follow.
	bytes := []byte{0x05}
	var c causalcontext.CausalContext

	// act
	err := c.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestCausalContext_UnmarshalRejectsExceptionAtOrBelowNormals(t *testing.T) {
	// arrange. normals {n1: 2}, then an exception (n1, 3). 3 is normals+1, so it
	// would have folded into normals and has no place as an exception.
	bytes := []byte{
		0x05,                       // normals length 5
		0x01, 0x02, 'n', '1', 0x02, // normals {n1: 2}: count 1, then n1 value 2
		0x01,                 // exception count 1
		0x02, 'n', '1', 0x03, // exception (n1, 3)
	}
	var c causalcontext.CausalContext

	// act
	err := c.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestCausalContext_UnmarshalRejectsUnsortedExceptions(t *testing.T) {
	// arrange. Empty normals, then exceptions (n2, 5) before (n1, 5), out of order.
	bytes := []byte{
		0x01, 0x00, // normals length 1, then empty version vector
		0x02,                 // exception count 2
		0x02, 'n', '2', 0x05, // exception (n2, 5)
		0x02, 'n', '1', 0x05, // exception (n1, 5)
	}
	var c causalcontext.CausalContext

	// act
	err := c.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestCausalContext_UnmarshalRejectsDuplicateExceptions(t *testing.T) {
	// arrange. Empty normals, then the same exception (n1, 5) twice. Equal
	// violates strictly increasing.
	bytes := []byte{
		0x01, 0x00,
		0x02,
		0x02, 'n', '1', 0x05,
		0x02, 'n', '1', 0x05,
	}
	var c causalcontext.CausalContext

	// act
	err := c.Unmarshal(bytes)

	// assert
	require.Error(t, err)
}

func TestCausalContext_UnmarshalReplacesExceptionsOnReuse(t *testing.T) {
	// Decoding into a reused variable must not keep exceptions from a previous
	// decode.
	// arrange
	withException, err := causalcontext.New().Observe("n1", 5).Marshal()
	require.NoError(t, err)
	noException, err := causalcontext.New().Observe("n1", 1).Marshal()
	require.NoError(t, err)

	var decoded causalcontext.CausalContext
	require.NoError(t, decoded.Unmarshal(withException))
	require.True(t, decoded.Contains("n1", 5)) // sanity check: the first decode holds it

	// act
	require.NoError(t, decoded.Unmarshal(noException))

	// assert
	require.False(t, decoded.Contains("n1", 5))
}
