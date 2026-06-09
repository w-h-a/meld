package orset_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/orset"
)

// --- construction, accessors, immutability ---

func TestSet_NewIsEmpty(t *testing.T) {
	// arrange + act
	s := orset.New[string]()

	// assert
	require.False(t, s.Contains("any"))
	require.Empty(t, s.Elements())
}

func TestSet_AddReturnsNewSetWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := orset.New[string]()

	// act
	b := a.Add("n1", "nginx")

	// assert
	require.False(t, a.Contains("nginx"))
	require.True(t, b.Contains("nginx"))
	require.ElementsMatch(t, []string{"nginx"}, b.Elements())
}

func TestSet_RemoveReturnsNewSetWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := orset.New[string]().Add("n1", "nginx")

	// act
	b := a.Remove("nginx")

	// assert
	require.True(t, a.Contains("nginx"))
	require.False(t, b.Contains("nginx"))
}

func TestSet_RemoveOfUnobservedElementIsNoOp(t *testing.T) {
	// arrange
	s := orset.New[string]()

	// act
	s2 := s.Remove("ghost")

	// assert
	require.False(t, s2.Contains("ghost"))
	require.Empty(t, s2.Elements())
}

func TestSet_CloneIsIndependent(t *testing.T) {
	// arrange
	a := orset.New[string]().Add("n1", "nginx")

	// act. Mutating the clone must not affect the original.
	b := a.Clone()
	b = b.Remove("nginx")

	// assert
	require.True(t, a.Contains("nginx"))
	require.False(t, b.Contains("nginx"))
}

// --- add-wins policy ---

func TestSet_ConcurrentAddRemoveYieldsAddWins(t *testing.T) {
	// arrange. n1 adds "nginx" and n2 mirrors the state.
	n1 := orset.New[string]().Add("n1", "nginx")
	n2 := n1.Clone()

	// n2 removes "nginx", which drops (nginx, n1, 1) from live but
	// retains V[n1]=1.
	n2 = n2.Remove("nginx")
	require.False(t, n2.Contains("nginx"))

	// Concurrently, n1 re-adds "nginx", minting (nginx, n1, 2). n2
	// has never observed this counter.
	n1 = n1.Add("n1", "nginx")
	require.True(t, n1.Contains("nginx"))

	// act. Merge in both orders.
	ab := n1.Merge(n2)
	ba := n2.Merge(n1)

	// assert. The concurrent add wins on both sides.
	require.True(t, ab.Contains("nginx"))
	require.True(t, ba.Contains("nginx"))
}

func TestSet_RemoveOnlyDeletesObservedTags(t *testing.T) {
	// arrange. Two adds at different nodes for the same element.
	// Neither replica has observed the other.
	a := orset.New[string]().Add("n1", "x") // triple (x, n1, 1)
	b := orset.New[string]().Add("n2", "x") // triple (x, n2, 1)

	// act. n1 removes "x" before seeing n2's add. Then merge.
	aRemoved := a.Remove("x")
	require.False(t, aRemoved.Contains("x"))
	merged := aRemoved.Merge(b)

	// assert. aRemoved's V[n2]=0, so the merge keeps b's
	// (x, n2, 1) because counter 1 > 0. "x" remains present.
	require.True(t, merged.Contains("x"))
}

// --- convergence under partition ---

func TestSet_PartitionAndHealConverges(t *testing.T) {
	// arrange. Two replicas diverge during a partition.
	a := orset.New[string]().
		Add("n1", "nginx").
		Add("n1", "ssh")
	b := orset.New[string]().
		Add("n2", "caddy").
		Add("n2", "envoy")
	b = b.Remove("envoy")

	// act. Heal in both directions.
	ab := a.Merge(b)
	ba := b.Merge(a)

	// assert. Both sides have the same view, and removed elements
	// stay removed.
	require.ElementsMatch(t, []string{"nginx", "ssh", "caddy"}, ab.Elements())
	require.ElementsMatch(t, ab.Elements(), ba.Elements())
}

// --- flock workload scenario ---

func TestSet_FlockWorkloadAddCancelRecycle(t *testing.T) {
	// arrange. flock-style: add workload, cancel, re-add same id.
	n1 := orset.New[string]()
	n2 := orset.New[string]()

	// n1 submits workload "nginx". n2 receives it.
	n1 = n1.Add("n1", "nginx")
	n2 = n2.Merge(n1)
	require.True(t, n2.Contains("nginx"))

	// n2 cancels nginx. n1 receives the cancellation.
	n2 = n2.Remove("nginx")
	n1 = n1.Merge(n2)
	require.False(t, n1.Contains("nginx"))

	// act. n1 re-submits nginx (operator retried). The new triple
	// is (nginx, n1, 2), which n2 has not observed.
	n1 = n1.Add("n1", "nginx")
	n2 = n2.Merge(n1)

	// assert. The new submission is live across both nodes.
	require.True(t, n1.Contains("nginx"))
	require.True(t, n2.Contains("nginx"))
}

// --- coalescing of repeated adds (Bieniusa Figure 3, set O) ---

func TestSet_AddCoalescesRepeatedAddsAtSameReplica(t *testing.T) {
	// arrange. Adding the same element at the same replica many
	// times should leave one triple in live, not many.
	once := orset.New[string]().Add("n1", "x")
	manyTimes := orset.New[string]()
	for range 5 {
		manyTimes = manyTimes.Add("n1", "x")
	}

	// act
	onceBytes, err := once.Marshal(stringEncode)
	require.NoError(t, err)
	manyBytes, err := manyTimes.Marshal(stringEncode)
	require.NoError(t, err)

	// assert
	require.Equal(t, len(onceBytes), len(manyBytes))
}

// --- crdt properties: commutative, associative, idempotent ---

func TestSet_MergeIsCommutative(t *testing.T) {
	cases := []struct {
		name string
		a, b orset.Set[string]
	}{
		{
			"both empty",
			orset.New[string](),
			orset.New[string](),
		},
		{
			"one side empty",
			orset.New[string](),
			orset.New[string]().Add("n1", "x"),
		},
		{
			"disjoint adds",
			orset.New[string]().Add("n1", "x"),
			orset.New[string]().Add("n2", "y"),
		},
		{
			"overlapping adds, same element, different nodes",
			orset.New[string]().Add("n1", "x"),
			orset.New[string]().Add("n2", "x"),
		},
		{
			"concurrent add and remove with re-add",
			orset.New[string]().Add("n1", "x").Add("n1", "x"),
			orset.New[string]().Add("n1", "x").Remove("x"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab := c.a.Merge(c.b)
			ba := c.b.Merge(c.a)

			// assert
			require.Equal(t, sortedElements(ab), sortedElements(ba))
		})
	}
}

func TestSet_MergeIsAssociative(t *testing.T) {
	cases := []struct {
		name    string
		a, b, c orset.Set[string]
	}{
		{
			"three disjoint adds",
			orset.New[string]().Add("n1", "x"),
			orset.New[string]().Add("n2", "y"),
			orset.New[string]().Add("n3", "z"),
		},
		{
			"adds and removes across three sides",
			orset.New[string]().Add("n1", "x").Add("n1", "y"),
			orset.New[string]().Add("n2", "x").Remove("x"),
			orset.New[string]().Add("n3", "z").Add("n3", "y").Remove("y"),
		},
		{
			"one input empty",
			orset.New[string](),
			orset.New[string]().Add("n1", "x"),
			orset.New[string]().Add("n2", "y"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left := c.a.Merge(c.b).Merge(c.c)
			right := c.a.Merge(c.b.Merge(c.c))

			// assert
			require.Equal(t, sortedElements(left), sortedElements(right))
		})
	}
}

func TestSet_MergeIsIdempotent(t *testing.T) {
	cases := []struct {
		name string
		a, b orset.Set[string]
	}{
		{
			"merging an empty set",
			orset.New[string]().Add("n1", "x"),
			orset.New[string](),
		},
		{
			"merging a concurrent set",
			orset.New[string]().Add("n1", "x"),
			orset.New[string]().Add("n2", "y"),
		},
		{
			"merging after a remove",
			orset.New[string]().Add("n1", "x").Add("n1", "y"),
			orset.New[string]().Add("n1", "x").Remove("x"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			twice := once.Merge(c.b)

			// assert
			require.Equal(t, sortedElements(once), sortedElements(twice))
		})
	}
}

func sortedElements(s orset.Set[string]) []string {
	out := s.Elements()
	sort.Strings(out)
	return out
}

// --- marshal / unmarshal ---

func stringEncode(s string) ([]byte, error) { return []byte(s), nil }
func stringDecode(b []byte) (string, error) { return string(b), nil }

func TestSet_MarshalUnmarshalRoundTripLiveElements(t *testing.T) {
	// arrange
	original := orset.New[string]().
		Add("n1", "nginx").
		Add("n2", "caddy")

	// act
	bytes, err := original.Marshal(stringEncode)
	require.NoError(t, err)
	var decoded orset.Set[string]
	require.NoError(t, decoded.Unmarshal(bytes, stringDecode))

	// assert
	require.ElementsMatch(t, original.Elements(), decoded.Elements())
}

func TestSet_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := orset.New[string]()

	// act
	bytes, err := original.Marshal(stringEncode)
	require.NoError(t, err)
	var decoded orset.Set[string]
	require.NoError(t, decoded.Unmarshal(bytes, stringDecode))

	// assert
	require.Empty(t, decoded.Elements())
}

func TestSet_MarshalUnmarshalRoundTripPreservesRemovalHistory(t *testing.T) {
	// arrange. State has live elements and one removed element.
	// The version vector records V[n1]=2 and V[n2]=1.
	original := orset.New[string]().
		Add("n1", "nginx").
		Add("n2", "envoy").
		Add("n1", "ssh")
	original = original.Remove("nginx")

	// act. Round-trip.
	bytes, err := original.Marshal(stringEncode)
	require.NoError(t, err)
	var decoded orset.Set[string]
	require.NoError(t, decoded.Unmarshal(bytes, stringDecode))

	// assert. Live state matches.
	require.ElementsMatch(t, original.Elements(), decoded.Elements())

	// The removal of "nginx" survives merge with a stale replica
	// that re-adds nginx at the same triple. decoded's V[n1]=2
	// means it has already observed counter 1 at n1, so the merge
	// drops stale's (nginx, n1, 1).
	stale := orset.New[string]().Add("n1", "nginx")
	merged := decoded.Merge(stale)
	require.False(t, merged.Contains("nginx"))
}

func TestSet_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var s orset.Set[string]

	// act
	err := s.Unmarshal(nil, stringDecode)

	// assert
	require.Error(t, err)
}

func TestSet_UnmarshalRejectsTruncatedVectorBytes(t *testing.T) {
	// arrange. vectorLen=5 but only 1 byte follows.
	bytes := []byte{0x05, 0x00}
	var s orset.Set[string]

	// act
	err := s.Unmarshal(bytes, stringDecode)

	// assert
	require.Error(t, err)
}

func TestSet_UnmarshalRejectsTruncatedElement(t *testing.T) {
	// arrange. vectorLen=1, empty vector, liveCount=1, elementLen=10,
	// no element bytes follow.
	bytes := []byte{
		0x01, // vectorLen = 1
		0x00, // empty vector (entry count = 0)
		0x01, // liveCount = 1
		0x0a, // elementLen = 10, no bytes follow
	}
	var s orset.Set[string]

	// act
	err := s.Unmarshal(bytes, stringDecode)

	// assert
	require.Error(t, err)
}

func TestSet_UnmarshalSurfacesDecoderError(t *testing.T) {
	// arrange. Valid bytes. Decoder rejects the element bytes.
	original := orset.New[string]().Add("n1", "nginx")
	bytes, err := original.Marshal(stringEncode)
	require.NoError(t, err)

	rejectingDecoder := func([]byte) (string, error) {
		return "", errors.New("nope")
	}
	var s orset.Set[string]

	// act
	err = s.Unmarshal(bytes, rejectingDecoder)

	// assert
	require.Error(t, err)
}

func TestSet_MarshalSurfacesEncoderError(t *testing.T) {
	// arrange
	s := orset.New[string]().Add("n1", "nginx")
	rejectingEncoder := func(string) ([]byte, error) {
		return nil, errors.New("nope")
	}

	// act
	_, err := s.Marshal(rejectingEncoder)

	// assert
	require.Error(t, err)
}
