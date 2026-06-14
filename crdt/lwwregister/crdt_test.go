package lwwregister_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/lwwregister"
)

// --- construction, accessors, immutability ---

func TestLWWRegister_NewIsEmpty(t *testing.T) {
	// arrange + act
	r := lwwregister.New[string]()

	// assert
	require.Equal(t, "", r.Value())
	require.Equal(t, lwwregister.Tag{}, r.Tag())
}

func TestLWWRegister_SetReturnsNewRegisterWithoutMutatingReceiver(t *testing.T) {
	// arrange
	a := lwwregister.New[string]()

	// act
	b := a.Set("n1", "v1")

	// assert
	require.Equal(t, "", a.Value())
	require.Equal(t, lwwregister.Tag{}, a.Tag())
	require.Equal(t, "v1", b.Value())
	require.Equal(t, lwwregister.Tag{Counter: 1, Writer: "n1"}, b.Tag())
}

func TestLWWRegister_SetAdvancesTagCounterByOneAcrossWriters(t *testing.T) {
	// arrange + act. Three Sets across two writers.
	r := lwwregister.New[string]().
		Set("n1", "v1"). // Counter 0+1 = 1
		Set("n2", "vX"). // Counter 1+1 = 2
		Set("n1", "v2")  // Counter 2+1 = 3

	// assert.
	require.Equal(t, "v2", r.Value())
	require.Equal(t, lwwregister.Tag{Counter: 3, Writer: "n1"}, r.Tag())
}

func TestLWWRegister_Merge_DuplicateWriterBreaksCommutativity(t *testing.T) {
	// arrange. Two replicas both call Set with the same writer.
	a := lwwregister.New[string]().Set("shared", "v1")
	b := lwwregister.New[string]().Set("shared", "v2")

	// act
	ab := a.Merge(b)
	ba := b.Merge(a)

	// assert. Identical Tags, different values, order-dependent
	// outcome.
	require.Equal(t, ab.Tag(), ba.Tag())
	require.NotEqual(t, ab.Value(), ba.Value())
}

// --- merge: lwwregister outcome ---

func TestLWWRegister_Merge_CausalChain_LaterWriteWins(t *testing.T) {
	// arrange. n2 observes n1's write, then writes again.
	a := lwwregister.New[string]().Set("n1", "v1") // tag=(1, n1)
	b := a.Set("n2", "v2")                         // tag=(2, n2)

	// act
	merged := a.Merge(b)

	// assert.
	require.Equal(t, "v2", merged.Value())
	require.Equal(t, lwwregister.Tag{Counter: 2, Writer: "n2"}, merged.Tag())
}

func TestLWWRegister_Merge_ConcurrentWrites_WriterTiebreakDecides(t *testing.T) {
	// arrange. Two disjoint writers. No causal relation.
	a := lwwregister.New[string]().Set("n1", "v1") // tag=(1, n1)
	b := lwwregister.New[string]().Set("n2", "v2") // tag=(1, n2)

	// act
	merged := a.Merge(b)

	// assert.
	require.Equal(t, "v2", merged.Value())
	require.Equal(t, lwwregister.Tag{Counter: 1, Writer: "n2"}, merged.Tag())
}

func TestLWWRegister_TagAdvancesAcrossCausallyOrderedWrites(t *testing.T) {
	// arrange. b is a local write that happens-after a. To build
	// c, we merge b into an unrelated concurrent register and
	// then do one more local Set on top of the merged state.
	a := lwwregister.New[string]().Set("n1", "v1")     // tag=(1, n1)
	b := a.Set("n2", "v2")                             // tag=(2, n2)
	other := lwwregister.New[string]().Set("n3", "v3") // tag=(1, n3), concurrent
	merged := other.Merge(b)                           // tag=(2, n2), b wins

	// act
	c := merged.Set("n2", "v4") // tag=(3, n2), local write

	// assert.
	require.True(t, a.Tag().Less(b.Tag()))
	require.True(t, b.Tag().Less(c.Tag()))
	require.True(t, a.Tag().Less(c.Tag()))
}

// --- crdt properties: commutative, associative, idempotent ---

func TestLWWRegister_MergeIsCommutative(t *testing.T) {
	// Order of arguments does not matter
	cases := []struct {
		name string
		a, b lwwregister.LWWRegister[string]
	}{
		{
			"disjoint writers, concurrent",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n2", "v2"),
		},
		{
			"causal chain",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n1", "v1").Set("n2", "v2"),
		},
		{
			"one side empty",
			lwwregister.New[string](),
			lwwregister.New[string]().Set("n1", "v1"),
		},
		{
			"both sides empty",
			lwwregister.New[string](),
			lwwregister.New[string](),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			ab := c.a.Merge(c.b)
			ba := c.b.Merge(c.a)

			// assert
			require.Equal(t, ab.Value(), ba.Value())
			require.Equal(t, ab.Tag(), ba.Tag())
		})
	}
}

func TestLWWRegister_MergeIsAssociative(t *testing.T) {
	// Grouping does not matter.
	cases := []struct {
		name    string
		a, b, c lwwregister.LWWRegister[string]
	}{
		{
			"three pairwise concurrent writes",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n2", "v2"),
			lwwregister.New[string]().Set("n3", "v3"),
		},
		{
			"causal chain with a concurrent third",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n1", "v1").Set("n2", "v2"),
			lwwregister.New[string]().Set("n3", "v3"),
		},
		{
			"one input empty",
			lwwregister.New[string](),
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n2", "v2"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			left := c.a.Merge(c.b).Merge(c.c)
			right := c.a.Merge(c.b.Merge(c.c))

			// assert
			require.Equal(t, left.Value(), right.Value())
			require.Equal(t, left.Tag(), right.Tag())
		})
	}
}

func TestLWWRegister_MergeIsIdempotent(t *testing.T) {
	// Merging the same value twice has the same effect as merging it
	// once.
	cases := []struct {
		name string
		a, b lwwregister.LWWRegister[string]
	}{
		{
			"merging an empty register",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string](),
		},
		{
			"merging a concurrent register",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n2", "v2"),
		},
		{
			"merging a strictly later register",
			lwwregister.New[string]().Set("n1", "v1"),
			lwwregister.New[string]().Set("n1", "v1").Set("n1", "v1"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act
			once := c.a.Merge(c.b)
			twice := once.Merge(c.b)

			// assert
			require.Equal(t, once.Value(), twice.Value())
			require.Equal(t, once.Tag(), twice.Tag())
		})
	}
}

// --- marshal / unmarshal ---

func TestLWWRegister_MarshalUnmarshalRoundTrip(t *testing.T) {
	// arrange
	original := lwwregister.New[string]().Set("n1", "v1").Set("n2", "v2")

	// act
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded lwwregister.LWWRegister[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert
	require.Equal(t, original.Value(), decoded.Value())
	require.Equal(t, original.Tag(), decoded.Tag())
}

func TestLWWRegister_MarshalUnmarshalRoundTripEmpty(t *testing.T) {
	// arrange
	original := lwwregister.New[string]()

	// act
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)
	var decoded lwwregister.LWWRegister[string]
	require.NoError(t, decoded.Unmarshal(bytes, crdt.StringDecode))

	// assert
	require.Equal(t, "", decoded.Value())
	require.Equal(t, lwwregister.Tag{}, decoded.Tag())
}

func TestLWWRegister_UnmarshalRejectsEmptyInput(t *testing.T) {
	// arrange
	var r lwwregister.LWWRegister[string]

	// act
	err := r.Unmarshal(nil, crdt.StringDecode)

	// assert
	require.Error(t, err)
}

func TestLWWRegister_UnmarshalRejectsTruncatedValueBytes(t *testing.T) {
	// arrange. valueLen header claims 10 bytes, none follow.
	bytes := []byte{0x0a}
	var r lwwregister.LWWRegister[string]

	// act
	err := r.Unmarshal(bytes, crdt.StringDecode)

	// assert
	require.Error(t, err)
}

func TestLWWRegister_UnmarshalSurfacesDecoderError(t *testing.T) {
	// arrange. Valid header. Decoder rejects the value bytes.
	original := lwwregister.New[string]().Set("n1", "v1")
	bytes, err := original.Marshal(crdt.StringEncode)
	require.NoError(t, err)

	rejectingDecoder := func([]byte) (string, error) {
		return "", errors.New("nope")
	}
	var r lwwregister.LWWRegister[string]

	// act
	err = r.Unmarshal(bytes, rejectingDecoder)

	// assert
	require.Error(t, err)
}

func TestLWWRegister_MarshalSurfacesEncoderError(t *testing.T) {
	// arrange
	r := lwwregister.New[string]().Set("n1", "v1")
	rejectingEncoder := func(string) ([]byte, error) {
		return nil, errors.New("nope")
	}

	// act
	_, err := r.Marshal(rejectingEncoder)

	// assert
	require.Error(t, err)
}
