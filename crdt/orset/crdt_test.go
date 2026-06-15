package orset_test

import (
	"encoding/binary"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/orset"
	"github.com/w-h-a/meld/crdt/versionvector"
)

// --- construction, accessors, immutability ---

func TestORSet_NewIsEmpty(t *testing.T) {
	// arrange + act
	s := orset.New[string]()

	// assert
	require.False(t, s.Contains("any"))
	require.Empty(t, s.Elements())
}

func TestORSet_AddReturnsNewSetWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := orset.New[string]()

	// act
	b := a.Add("n1", "nginx")

	// assert
	require.False(t, a.Contains("nginx"))
	require.True(t, b.Contains("nginx"))
	require.ElementsMatch(t, []string{"nginx"}, b.Elements())
}

func TestORSet_RemoveReturnsNewSetWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := orset.New[string]().Add("n1", "nginx")

	// act
	b := a.Remove("nginx")

	// assert
	require.True(t, a.Contains("nginx"))
	require.False(t, b.Contains("nginx"))
}

func TestORSet_RemoveOfUnobservedElementIsNoOp(t *testing.T) {
	// arrange
	s := orset.New[string]()

	// act
	s2 := s.Remove("ghost")

	// assert
	require.False(t, s2.Contains("ghost"))
	require.Empty(t, s2.Elements())
}

func TestORSet_CloneIsIndependent(t *testing.T) {
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

func TestORSet_ConcurrentAddRemoveYieldsAddWins(t *testing.T) {
	// arrange. n1 adds "nginx" and n2 mirrors the state.
	n1 := orset.New[string]().Add("n1", "nginx")
	n2 := n1.Clone()

	// n2 removes "nginx", which drops (nginx, n1, 1) from live but
	// keeps the dot (n1, 1) in seen.
	n2 = n2.Remove("nginx")
	require.False(t, n2.Contains("nginx"))

	// Concurrently, n1 re-adds "nginx", minting (nginx, n1, 2). n2
	// has never seen the dot (n1, 2).
	n1 = n1.Add("n1", "nginx")
	require.True(t, n1.Contains("nginx"))

	// act. Merge in both orders.
	ab := n1.Merge(n2)
	ba := n2.Merge(n1)

	// assert. The concurrent add wins on both sides.
	require.True(t, ab.Contains("nginx"))
	require.True(t, ba.Contains("nginx"))
}

func TestORSet_RemoveOnlyDeletesObservedTags(t *testing.T) {
	// arrange. Two adds at different nodes for the same element.
	// Neither replica has observed the other.
	a := orset.New[string]().Add("n1", "x") // triple (x, n1, 1)
	b := orset.New[string]().Add("n2", "x") // triple (x, n2, 1)

	// act. n1 removes "x" before seeing n2's add. Then merge.
	aRemoved := a.Remove("x")
	require.False(t, aRemoved.Contains("x"))
	merged := aRemoved.Merge(b)

	// assert. aRemoved has never seen the dot (n2, 1), so the merge
	// keeps b's (x, n2, 1). "x" remains present.
	require.True(t, merged.Contains("x"))
}

// --- convergence under partition ---

func TestORSet_PartitionAndHealConverges(t *testing.T) {
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

func TestORSet_FlockWorkloadAddCancelRecycle(t *testing.T) {
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

func TestORSet_AddCoalescesRepeatedAddsAtSameReplica(t *testing.T) {
	// arrange. Adding the same element at the same replica many
	// times should leave one triple in live, not many.
	once := orset.New[string]().Add("n1", "x")
	manyTimes := orset.New[string]()
	for range 5 {
		manyTimes = manyTimes.Add("n1", "x")
	}

	// act
	onceBytes, err := once.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	manyBytes, err := manyTimes.Marshal(crdt.StringEncode)
	require.NoError(t, err)

	// assert
	require.Equal(t, len(onceBytes), len(manyBytes))
}

// --- crdt properties: commutative, associative, idempotent ---

func TestORSet_MergeIsCommutative(t *testing.T) {
	cases := []struct {
		name string
		a, b orset.ORSet[string]
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

func TestORSet_MergeIsAssociative(t *testing.T) {
	cases := []struct {
		name    string
		a, b, c orset.ORSet[string]
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

func TestORSet_MergeIsIdempotent(t *testing.T) {
	cases := []struct {
		name string
		a, b orset.ORSet[string]
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

func sortedElements(s orset.ORSet[string]) []string {
	out := s.Elements()
	sort.Strings(out)
	return out
}

// --- delta ---

func TestORSet_AddDeltaMergesToSameAsAdd(t *testing.T) {
	// arrange
	s := orset.New[string]().Add("n1", "nginx").Add("n2", "caddy")

	// act
	viaDelta := s.Merge(s.AddDelta("n1", "nginx"))
	viaAdd := s.Add("n1", "nginx")

	// assert
	require.ElementsMatch(t, viaAdd.Elements(), viaDelta.Elements())
}

func TestORSet_RemoveDeltaMergesToSameAsRemove(t *testing.T) {
	// arrange
	s := orset.New[string]().Add("n1", "nginx").Add("n2", "caddy").Add("n1", "ssh")

	// act
	viaDelta := s.Merge(s.RemoveDelta("nginx"))
	viaAdd := s.Remove("nginx")

	// assert
	require.ElementsMatch(t, viaAdd.Elements(), viaDelta.Elements())
}

func TestORSet_AddDeltaWithGapConverges(t *testing.T) {
	// arrange
	rcv := orset.New[string]().Add("n1", "a")

	src := orset.New[string]().Add("n1", "a")
	newSrc := src.Add("n1", "b")
	deltaB := src.AddDelta("n1", "b")
	deltaC := newSrc.AddDelta("n1", "c")

	// act
	rcv = rcv.Merge(deltaC)
	require.True(t, rcv.Contains("c"))
	rcv = rcv.Merge(deltaB)

	// assert
	full := orset.New[string]().Add("n1", "a").Add("n1", "b").Add("n1", "c")
	require.ElementsMatch(t, full.Elements(), rcv.Elements())
}

func TestORSet_ConcurrentAddDeltaRemoveDeltaYieldsAddWins(t *testing.T) {
	// arrange
	base := orset.New[string]().Add("n1", "nginx")
	n1 := base.Clone()
	n2 := base.Clone()

	addDelta := n1.AddDelta("n1", "nginx")
	rmDelta := n2.RemoveDelta("nginx")

	// act
	ab := base.Merge(addDelta).Merge(rmDelta)
	ba := base.Merge(rmDelta).Merge(addDelta)

	// assert
	require.True(t, ab.Contains("nginx"))
	require.True(t, ba.Contains("nginx"))
}

func TestORSet_RemoveDeltaOfAbsentElementIsNoOP(t *testing.T) {
	// arrange
	s := orset.New[string]().Add("n1", "nginx")

	// act
	merged := s.Merge(s.RemoveDelta("ghost"))

	// assert
	require.ElementsMatch(t, s.Elements(), merged.Elements())
}

// --- marshal / unmarshal ---

func TestORSet_MarshalUnmarshalRoundTripLiveElements(t *testing.T) {
	// arrange
	original := orset.New[string]().
		Add("n1", "nginx").
		Add("n2", "caddy")

	// act
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded orset.ORSet[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert
	require.ElementsMatch(t, original.Elements(), decoded.Elements())
}

func TestORSet_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := orset.New[string]()

	// act
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded orset.ORSet[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert
	require.Empty(t, decoded.Elements())
}

func TestORSet_MarshalUnmarshalRoundTripPreservesRemovalHistory(t *testing.T) {
	// arrange. State has live elements and one removed element. The
	// context has seen n1's dots 1 and 2, and n2's dot 1.
	original := orset.New[string]().
		Add("n1", "nginx").
		Add("n2", "envoy").
		Add("n1", "ssh")
	original = original.Remove("nginx")

	// act. Round-trip.
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded orset.ORSet[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert. Live state matches.
	require.ElementsMatch(t, original.Elements(), decoded.Elements())

	// The removal of "nginx" survives merge with a stale replica
	// that re-adds nginx at the same triple. decoded has seen the
	// dot (n1, 1), so the merge drops stale's (nginx, n1, 1).
	stale := orset.New[string]().Add("n1", "nginx")
	merged := decoded.Merge(stale)
	require.False(t, merged.Contains("nginx"))
}

func TestORSet_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var s orset.ORSet[string]

	// act
	err := s.Unmarshal(nil, crdt.StringDecode)

	// assert
	require.Error(t, err)
}

func TestORSet_UnmarshalRejectsTruncatedContextBytes(t *testing.T) {
	// arrange. contextLen=5 but only 1 byte follows.
	bytes := []byte{0x05, 0x00}
	var s orset.ORSet[string]

	// act
	err := s.Unmarshal(bytes, crdt.StringDecode)

	// assert
	require.Error(t, err)
}

func TestORSet_UnmarshalRejectsTruncatedElement(t *testing.T) {
	// arrange. A valid empty causal context, then liveCount=1, then a triple
	// whose elementLen claims 10 bytes but none follow. The bytes must reach
	// the element decode and fail there, not earlier at the context parse.
	bytes := []byte{
		0x03, // contextLen = 3
		0x01, // normalsLen = 1
		0x00, // empty version vector (entry count = 0)
		0x00, // exception count = 0
		0x01, // liveCount = 1
		0x0a, // elementLen = 10, no bytes follow
	}
	var s orset.ORSet[string]

	// act
	err := s.Unmarshal(bytes, crdt.StringDecode)

	// assert. The failure is element truncation, not a context parse error.
	require.ErrorContains(t, err, "element bytes truncated")
}

func TestORSet_UnmarshalSurfacesDecoderError(t *testing.T) {
	// arrange. Valid bytes. Decoder rejects the element bytes.
	original := orset.New[string]().Add("n1", "nginx")
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)

	rejectingDecoder := func([]byte) (string, error) {
		return "", errors.New("nope")
	}
	var s orset.ORSet[string]

	// act
	err = s.Unmarshal(bytes, rejectingDecoder)

	// assert
	require.Error(t, err)
}

func TestORSet_MarshalSurfacesEncoderError(t *testing.T) {
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

func TestORSet_MarshalContextOverheadIsConstant(t *testing.T) {
	// The causal context adds a fixed handful of bytes over a bare version
	// vector, a length prefix and a zero exception count, regardless of how many
	// nodes the set has touched. So the context section of the marshal is the
	// version vector plus a small constant, not growth proportional to state.
	// arrange. An ORSet touched by three nodes, and the version vector that
	// matches its observed dots.
	s := orset.New[string]().
		Add("n1", "x").Add("n1", "y").
		Add("n2", "z").
		Add("n3", "w")
	vv := versionvector.New().
		Increment("n1").Increment("n1").
		Increment("n2").
		Increment("n3")

	// act
	sBytes, err := s.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	vvBytes, err := vv.Marshal()
	require.NoError(t, err)

	// The leading uvarint of the ORSet marshal is the length of its context
	// section.
	seenLen, n := binary.Uvarint(sBytes)
	require.Positive(t, n)

	// assert. The context section is the version vector plus a small constant.
	require.GreaterOrEqual(t, seenLen, uint64(len(vvBytes)))
	require.LessOrEqual(t, seenLen-uint64(len(vvBytes)), uint64(3))
}

func TestORSet_MergeThenMarshalUnmarshalPreservesState(t *testing.T) {
	// A merged multi-replica state round-trips: marshalling and unmarshalling
	// preserves both the live elements and the merged causal context.
	// arrange. Two replicas, each with its own dots, one with a removal.
	a := orset.New[string]().Add("n1", "nginx").Add("n1", "ssh")
	b := orset.New[string]().Add("n2", "caddy").Add("n2", "envoy")
	b = b.Remove("envoy")
	merged := a.Merge(b)

	// act
	bytes, err := merged.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded orset.ORSet[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert. The live elements survive the round-trip.
	require.ElementsMatch(t, merged.Elements(), decoded.Elements())

	// And the merged context survived: a stale replica re-adding envoy at its
	// old dot stays removed, because decoded still knows it observed that dot.
	stale := orset.New[string]().Add("n2", "caddy").Add("n2", "envoy")
	require.False(t, decoded.Merge(stale).Contains("envoy"))
}

func TestORSet_DeltaMarshalsSmallerThanFullState(t *testing.T) {
	// arrange
	s := orset.New[string]().
		Add("n1", "nginx").
		Add("n2", "caddy").
		Add("n1", "ssh").
		Add("n3", "envoy")

	// act
	full, err := s.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	addDelta, err := s.AddDelta("n1", "redis").Marshal(crdt.StringEncode)
	require.NoError(t, err)
	removeDelta, err := s.RemoveDelta("nginx").Marshal(crdt.StringEncode)
	require.NoError(t, err)

	// assert
	require.Less(t, len(addDelta), len(full))
	require.Less(t, len(removeDelta), len(full))
}
