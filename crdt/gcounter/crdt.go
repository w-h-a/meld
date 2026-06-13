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

// GCounter is a grow-only counter. It holds one dot per node, keyed
// by node id. A node only ever raises its own dot, and the counter's
// value is the sum across all dots.
type GCounter struct {
	dots []crdt.Dot
}

func New() GCounter {
	return GCounter{}
}

// Get returns nodeID's dot, or 0 if nodeID has never incremented.
func (g GCounter) Get(nodeID string) uint64 {
	i := sort.Search(len(g.dots), func(i int) bool {
		return g.dots[i].Node >= nodeID
	})

	if i < len(g.dots) && g.dots[i].Node == nodeID {
		return g.dots[i].Counter
	}

	return 0
}

// Value returns the counter's reading, the sum of every node's dot.
func (g GCounter) Value() uint64 {
	var sum uint64

	for _, s := range g.dots {
		sum += s.Counter
	}

	return sum
}

// DotCount returns the length of dots.
func (g GCounter) DotCount() int {
	return len(g.dots)
}

// Increment returns a new counter with nodeID's dot raised by 1.
// The receiver is not modified; so, counters are safe to share across
// routines and the wire. Callers pass their own node id and only their own.
func (g GCounter) Increment(nodeID string) GCounter {
	i := sort.Search(len(g.dots), func(i int) bool {
		return g.dots[i].Node >= nodeID
	})

	if i < len(g.dots) && g.dots[i].Node == nodeID {
		out := make([]crdt.Dot, len(g.dots))
		copy(out, g.dots)
		out[i].Counter++

		return GCounter{dots: out}
	}

	out := make([]crdt.Dot, len(g.dots)+1)
	copy(out, g.dots[:i])
	out[i] = crdt.Dot{Node: nodeID, Counter: 1}
	copy(out[i+1:], g.dots[i:])

	return GCounter{dots: out}
}

// Clone returns a deep copy.
func (g GCounter) Clone() GCounter {
	if len(g.dots) == 0 {
		return GCounter{}
	}

	out := make([]crdt.Dot, len(g.dots))
	copy(out, g.dots)

	return GCounter{dots: out}
}

// Merge returns the counter that knows the largest count each node has
// reached. For every dot, it keeps the larger of the two inputs.
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
	out := make([]crdt.Dot, 0, len(g.dots)+len(other.dots))

	i, j := 0, 0
	for i < len(g.dots) && j < len(other.dots) {
		switch {
		case g.dots[i].Node == other.dots[j].Node:
			value := max(g.dots[i].Counter, other.dots[j].Counter)
			out = append(out, crdt.Dot{Node: g.dots[i].Node, Counter: value})
			i++
			j++
		case g.dots[i].Node < other.dots[j].Node:
			out = append(out, g.dots[i])
			i++
		default:
			out = append(out, other.dots[j])
			j++
		}
	}

	// drain the rest, if any
	out = append(out, g.dots[i:]...)
	out = append(out, other.dots[j:]...)

	return GCounter{dots: out}
}

// Marshal encodes the counter for persistence or the wire.
// Format: uvarint(dot count), then per dot uvarint(idLen), idBytes,
// and uvarint(value).
func (g GCounter) Marshal() ([]byte, error) {
	buf := make([]byte, 0, 1+len(g.dots)*12)
	buf = binary.AppendUvarint(buf, uint64(len(g.dots)))

	for _, s := range g.dots {
		buf = binary.AppendUvarint(buf, uint64(len(s.Node)))
		buf = append(buf, s.Node...)
		buf = binary.AppendUvarint(buf, s.Counter)
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

	out := make([]crdt.Dot, 0, count)
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
			return errors.New("gcounter: zero-valued dot violates invariant")
		}

		if k > 0 && id <= prev {
			return errors.New("gcounter: dots not sorted by id")
		}

		prev = id

		out = append(out, crdt.Dot{Node: id, Counter: value})
	}

	g.dots = out

	return nil
}
