// Package vclock implements version vectors for causal ordering.
// No wall clocks. Monotonically increasing per node.
//
// References:
//   - Lamport, "Time, Clocks, and the Ordering of Events" (1978)
//   - DDIA chapter 5 (detecting concurrent writes)
package vclock

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/w-h-a/meld/crdt"
)

// Make sure VectorClock satisfies crdt.Mergeable.
var _ crdt.Mergeable[VectorClock] = VectorClock{}

// VectorClock summarizes the events one replica has seen in a
// distributed system.
type VectorClock struct {
	entries []counter
}

func New() VectorClock {
	return VectorClock{}
}

// Get returns the counter for nodeID, or 0 if nodeID is absent
func (v VectorClock) Get(nodeID string) uint64 {
	i := sort.Search(len(v.entries), func(i int) bool {
		return v.entries[i].id >= nodeID
	})

	if i < len(v.entries) && v.entries[i].id == nodeID {
		return v.entries[i].value
	}

	return 0
}

// Increment returns a new vector with nodeID's counter raised by 1.
// The receiver is not modified; so, vectors are safe to share across
// routines and the wire. Callers pass their own node id and only their own.
func (v VectorClock) Increment(nodeID string) VectorClock {
	i := sort.Search(len(v.entries), func(i int) bool {
		return v.entries[i].id >= nodeID
	})

	if i < len(v.entries) && v.entries[i].id == nodeID {
		out := make([]counter, len(v.entries))
		copy(out, v.entries)
		out[i].value++

		return VectorClock{entries: out}
	}

	out := make([]counter, len(v.entries)+1)
	copy(out, v.entries[:i])
	out[i] = counter{id: nodeID, value: 1}
	copy(out[i+1:], v.entries[i:])

	return VectorClock{entries: out}
}

// Clone returns a deep copy.
func (v VectorClock) Clone() VectorClock {
	if len(v.entries) == 0 {
		return VectorClock{}
	}

	out := make([]counter, len(v.entries))
	copy(out, v.entries)

	return VectorClock{entries: out}
}

// Compare returns the causal relationship of v to other. The four
// outcomes answer the four practical questions a replica asks when
// it sees another replica's vector.
//
// Equal. Every counter matches. The replicas agree and no work is needed.
//
//	v = {n1: 1, n2: 1}
//	other = {n1: 1, n2: 1}
//
// Both sides have seen exactly the same events. So v.Compare(other)
// returns Equal.
//
// Greater. Every counter in v is at least the matching counter in other,
// and at least one is strictly greater. In this case, the caller can
// ignore the incoming state as a strict subset of its own.
//
//	v = {n1: 2, n2: 1}
//	other = {n1: 1}
//
// v has seen one more n1 event than other and one n2 event that other
// has not heard about. So v.Compare(other) returns Greater.
//
// Lesser. the mirror of Greater.
//
// ConcurrentGreater or ConcurrentLesser. Some counters favor v and
// some favor other. The replicas diverged. Each one saw events the
// other did not. The caller must merge or invoke a conflict policy.
//
//	v = {n1: 1}
//	other = {n2: 1}
//
// The split between ConcurrentGreater and ConcurrentLesser is a
// deterministic tiebreak so consumers that need a single answer
// pick the same winner always. The vector with the larger sum of
// counters wins. If the sums are equal, the sorted entries are walked
// and at each position the larger id wins, then the larger value, with
// the longer slice winning if every shared position matched.
func (v VectorClock) Compare(other VectorClock) Ordering {
	var hasGreater, hasLesser bool
	var vSum, oSum uint64

	i, j := 0, 0
	for i < len(v.entries) && j < len(other.entries) {
		switch {
		case v.entries[i].id == other.entries[j].id:
			if v.entries[i].value > other.entries[j].value {
				hasGreater = true
			} else if v.entries[i].value < other.entries[j].value {
				hasLesser = true
			}
			vSum += v.entries[i].value
			oSum += other.entries[j].value
			i++
			j++
		case v.entries[i].id < other.entries[j].id:
			hasGreater = true
			vSum += v.entries[i].value
			i++
		default:
			hasLesser = true
			oSum += other.entries[j].value
			j++
		}
	}

	for ; i < len(v.entries); i++ {
		hasGreater = true
		vSum += v.entries[i].value
	}

	for ; j < len(other.entries); j++ {
		hasLesser = true
		oSum += other.entries[j].value
	}

	switch {
	case !hasGreater && !hasLesser:
		return Equal
	case hasGreater && !hasLesser:
		return Greater
	case !hasGreater && hasLesser:
		return Lesser
	case vSum > oSum:
		return ConcurrentGreater
	case vSum < oSum:
		return ConcurrentLesser
	}

	if breakTie(v.entries, other.entries) > 0 {
		return ConcurrentGreater
	}

	return ConcurrentLesser
}

// Merge returns the smallest vector that knows everything either v
// or other knew. For each id, it keeps the larger counter. So Merge
// results in the union of two causal histories.
//
// Worked example.
//
//	v = {n1: 3, n2: 1}
//	other = {n1: 2, n2: 4, n3: 1}
//	merged = {n1: 3, n2: 4, n3: 1}
//
// In this example, v has seen 3 n1 events but only 1 n2 event.
// By contrast, other has seen 4 n2 events and is the only history
// that heard n3 events. After merging, the result knows the
// largest count for every id; so, the merged vector knows everything.
//
// Merge is commutative; so, the order that two messages arrive in
// does not matter. Merge is associative; so, a replica can fold
// many messages together in any grouping. Merge is idempotent; so,
// a duplicate message has no effect.
func (v VectorClock) Merge(other VectorClock) VectorClock {
	out := make([]counter, 0, len(v.entries)+len(other.entries))

	i, j := 0, 0
	for i < len(v.entries) && j < len(other.entries) {
		switch {
		case v.entries[i].id == other.entries[j].id:
			value := max(other.entries[j].value, v.entries[i].value)
			out = append(out, counter{id: v.entries[i].id, value: value})
			i++
			j++
		case v.entries[i].id < other.entries[j].id:
			out = append(out, v.entries[i])
			i++
		default:
			out = append(out, other.entries[j])
			j++
		}
	}

	// drain the rest, if any
	out = append(out, v.entries[i:]...)
	out = append(out, other.entries[j:]...)

	return VectorClock{entries: out}
}

// Marshal encodes the vector for persistence or the wire.
// Format: uvarint(entry count), then per-entry uvarint(idLen), idBytes,
// and uvarint(value).
func (v VectorClock) Marshal() ([]byte, error) {
	buf := make([]byte, 0, 1+len(v.entries)*12)
	buf = binary.AppendUvarint(buf, uint64(len(v.entries)))

	for _, e := range v.entries {
		buf = binary.AppendUvarint(buf, uint64(len(e.id)))
		buf = append(buf, e.id...)
		buf = binary.AppendUvarint(buf, e.value)
	}

	return buf, nil
}

// Unmarshal parses the byte form into the receiver and validates
// the invariants.
func (v *VectorClock) Unmarshal(data []byte) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("vclock: invalid count")
	}

	data = data[n:]

	if count > uint64(len(data)) {
		return errors.New("vclock: count exceeds remaining data")
	}

	out := make([]counter, 0, count)
	var prev string

	for k := range count {
		idLen, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("vclock: invalid id length")
		}

		data = data[n:]

		if uint64(len(data)) < idLen {
			return errors.New("vclock: id bytes truncated")
		}

		id := string(data[:idLen])

		data = data[idLen:]

		val, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("vclock: invalid value")
		}

		data = data[n:]

		if val == 0 {
			return errors.New("vclock: zero-valued entry violates invariant")
		}

		if k > 0 && id <= prev {
			return errors.New("vclock: entries not sorted by id")
		}

		prev = id

		out = append(out, counter{id: id, value: val})
	}

	v.entries = out

	return nil
}
