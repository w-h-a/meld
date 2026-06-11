// Package gcounter implements the Grow-only Counter CRDT.
// Each node increments its own slot. Value is the sum.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package gcounter

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/w-h-a/meld/crdt"
)

// Make sure GCounter satisfies crdt.Mergeable.
var _ crdt.Mergeable[GCounter] = GCounter{}

// GCounter is a grow-only counter. It holds one slot per node, keyed
// by node id. A node only ever raises its own slot, and the counter's
// value is the sum across all slots.
type GCounter struct {
	slots []slot
}

func New() GCounter {
	return GCounter{}
}

// Get returns nodeID's slot, or 0 if nodeID has never incremented.
func (g GCounter) Get(nodeID string) uint64 {
	i := sort.Search(len(g.slots), func(i int) bool {
		return g.slots[i].id >= nodeID
	})

	if i < len(g.slots) && g.slots[i].id == nodeID {
		return g.slots[i].value
	}

	return 0
}

// Value returns the counter's reading, the sum of every node's slot.
func (g GCounter) Value() uint64 {
	var sum uint64

	for _, s := range g.slots {
		sum += s.value
	}

	return sum
}

// SlotCount returns the length of slots.
func (g GCounter) SlotCount() int {
	return len(g.slots)
}

// Increment returns a new counter with nodeID's slot raised by 1.
// The receiver is not modified; so, counters are safe to share across
// routines and the wire. Callers pass their own node id and only their own.
func (g GCounter) Increment(nodeID string) GCounter {
	i := sort.Search(len(g.slots), func(i int) bool {
		return g.slots[i].id >= nodeID
	})

	if i < len(g.slots) && g.slots[i].id == nodeID {
		out := make([]slot, len(g.slots))
		copy(out, g.slots)
		out[i].value++

		return GCounter{slots: out}
	}

	out := make([]slot, len(g.slots)+1)
	copy(out, g.slots[:i])
	out[i] = slot{id: nodeID, value: 1}
	copy(out[i+1:], g.slots[i:])

	return GCounter{slots: out}
}

// Clone returns a deep copy.
func (g GCounter) Clone() GCounter {
	if len(g.slots) == 0 {
		return GCounter{}
	}

	out := make([]slot, len(g.slots))
	copy(out, g.slots)

	return GCounter{slots: out}
}

// Merge returns the counter that knows the largest count each node has
// reached. For every slot, it keeps the larger of the two inputs.
//
// Worked example.
//
//	g      = {n1: 2, n2: 1}
//	other  = {n1: 1, n3: 3}
//	merged = {n1: 2, n2: 1, n3: 3}  value 6
//
// In this example, g has seen 2 n1 events but only 1 n2 event.
// By contrast, other has seen 1 n1 event and 3 n3 events. After merging,
// the result knows the largest count for every id.
//
// Merge is commutative; so, the order that two messages arrive in
// does not matter. Merge is associative; so, a replica can fold
// many messages together in any grouping. Merge is idempotent; so,
// a duplicate message has no effect.
func (g GCounter) Merge(other GCounter) GCounter {
	out := make([]slot, 0, len(g.slots)+len(other.slots))

	i, j := 0, 0
	for i < len(g.slots) && j < len(other.slots) {
		switch {
		case g.slots[i].id == other.slots[j].id:
			value := max(g.slots[i].value, other.slots[j].value)
			out = append(out, slot{id: g.slots[i].id, value: value})
			i++
			j++
		case g.slots[i].id < other.slots[j].id:
			out = append(out, g.slots[i])
			i++
		default:
			out = append(out, other.slots[j])
			j++
		}
	}

	// drain the rest, if any
	out = append(out, g.slots[i:]...)
	out = append(out, other.slots[j:]...)

	return GCounter{slots: out}
}

// Marshal encodes the counter for persistence or the wire.
// Format: uvarint(slot count), then per slot uvarint(idLen), idBytes,
// and uvarint(value).
func (g GCounter) Marshal() ([]byte, error) {
	buf := make([]byte, 0, 1+len(g.slots)*12)
	buf = binary.AppendUvarint(buf, uint64(len(g.slots)))

	for _, s := range g.slots {
		buf = binary.AppendUvarint(buf, uint64(len(s.id)))
		buf = append(buf, s.id...)
		buf = binary.AppendUvarint(buf, s.value)
	}

	return buf, nil
}

// Unmarshal parses the byte form into the receiver and validates the
// invariants.
func (g *GCounter) Unmarshal(data []byte) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("gcounter: invalid count")
	}

	data = data[n:]

	if count > uint64(len(data)) {
		return errors.New("gcounter: count exceeds remaining data")
	}

	out := make([]slot, 0, count)
	var prev string

	for k := range count {
		idLen, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("gcounter: invalid id length")
		}

		data = data[n:]

		if uint64(len(data)) < idLen {
			return errors.New("gcounter: id bytes truncated")
		}

		id := string(data[:idLen])

		data = data[idLen:]

		value, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("gcounter: invalid value")
		}

		data = data[n:]

		if value == 0 {
			return errors.New("gcounter: zero-valued slot violates invariant")
		}

		if k > 0 && id <= prev {
			return errors.New("gcounter: slots not sorted by id")
		}

		prev = id

		out = append(out, slot{id: id, value: value})
	}

	g.slots = out

	return nil
}
