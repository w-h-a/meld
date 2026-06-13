// Package versionvector implements version vectors for causal ordering.
// No wall clocks. Monotonically increasing per node.
//
// References:
//   - Lamport, "Time, Clocks, and the Ordering of Events" (1978)
//   - DDIA chapter 5 (detecting concurrent writes)
package versionvector

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/w-h-a/meld/crdt"
)

// Make sure VersionVector satisfies crdt.Mergeable.
var _ crdt.Mergeable[VersionVector] = VersionVector{}

// VersionVector summarizes the events one replica has seen in a
// distributed system.
type VersionVector struct {
	dots []crdt.Dot
}

func New() VersionVector {
	return VersionVector{}
}

// Get returns the counter for nodeID, or 0 if nodeID is absent
func (v VersionVector) Get(nodeID string) uint64 {
	i := sort.Search(len(v.dots), func(i int) bool {
		return v.dots[i].Node >= nodeID
	})

	if i < len(v.dots) && v.dots[i].Node == nodeID {
		return v.dots[i].Counter
	}

	return 0
}

// Increment returns a new vector with nodeID's counter raised by 1.
// The receiver is not modified; so, vectors are safe to share across
// routines and the wire. Callers pass their own node id and only their own.
func (v VersionVector) Increment(nodeID string) VersionVector {
	i := sort.Search(len(v.dots), func(i int) bool {
		return v.dots[i].Node >= nodeID
	})

	if i < len(v.dots) && v.dots[i].Node == nodeID {
		out := make([]crdt.Dot, len(v.dots))
		copy(out, v.dots)
		out[i].Counter++

		return VersionVector{dots: out}
	}

	out := make([]crdt.Dot, len(v.dots)+1)
	copy(out, v.dots[:i])
	out[i] = crdt.Dot{Node: nodeID, Counter: 1}
	copy(out[i+1:], v.dots[i:])

	return VersionVector{dots: out}
}

// Clone returns a deep copy.
func (v VersionVector) Clone() VersionVector {
	if len(v.dots) == 0 {
		return VersionVector{}
	}

	out := make([]crdt.Dot, len(v.dots))
	copy(out, v.dots)

	return VersionVector{dots: out}
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
// Lesser. The mirror of Greater.
//
// ConcurrentGreater or ConcurrentLesser. Some dots favor v and
// some favor other. The replicas diverged. Each one saw events the
// other did not. The caller must merge or invoke a conflict policy.
//
//	v = {n1: 1}
//	other = {n2: 1}
//
// The split between ConcurrentGreater and ConcurrentLesser is a
// deterministic tiebreak so consumers that need a single answer
// pick the same winner always. The vector with the larger sum of
// dots wins. If the sums are equal, the sorted dots are walked
// and at each position the larger id wins, then the larger value, with
// the longer slice winning if every shared position matched.
func (v VersionVector) Compare(other VersionVector) Ordering {
	var hasGreater, hasLesser bool
	var vSum, oSum uint64

	i, j := 0, 0
	for i < len(v.dots) && j < len(other.dots) {
		switch {
		case v.dots[i].Node == other.dots[j].Node:
			if v.dots[i].Counter > other.dots[j].Counter {
				hasGreater = true
			} else if v.dots[i].Counter < other.dots[j].Counter {
				hasLesser = true
			}
			vSum += v.dots[i].Counter
			oSum += other.dots[j].Counter
			i++
			j++
		case v.dots[i].Node < other.dots[j].Node:
			hasGreater = true
			vSum += v.dots[i].Counter
			i++
		default:
			hasLesser = true
			oSum += other.dots[j].Counter
			j++
		}
	}

	for ; i < len(v.dots); i++ {
		hasGreater = true
		vSum += v.dots[i].Counter
	}

	for ; j < len(other.dots); j++ {
		hasLesser = true
		oSum += other.dots[j].Counter
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

	if breakTie(v.dots, other.dots) > 0 {
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
func (v VersionVector) Merge(other VersionVector) VersionVector {
	out := make([]crdt.Dot, 0, len(v.dots)+len(other.dots))

	i, j := 0, 0
	for i < len(v.dots) && j < len(other.dots) {
		switch {
		case v.dots[i].Node == other.dots[j].Node:
			value := max(other.dots[j].Counter, v.dots[i].Counter)
			out = append(out, crdt.Dot{Node: v.dots[i].Node, Counter: value})
			i++
			j++
		case v.dots[i].Node < other.dots[j].Node:
			out = append(out, v.dots[i])
			i++
		default:
			out = append(out, other.dots[j])
			j++
		}
	}

	// drain the rest, if any
	out = append(out, v.dots[i:]...)
	out = append(out, other.dots[j:]...)

	return VersionVector{dots: out}
}

// Marshal encodes the vector for persistence or the wire.
// Format: uvarint(entry count), then per-entry uvarint(idLen), idBytes,
// and uvarint(value).
func (v VersionVector) Marshal() ([]byte, error) {
	buf := make([]byte, 0, 1+len(v.dots)*12)
	buf = binary.AppendUvarint(buf, uint64(len(v.dots)))

	for _, e := range v.dots {
		buf = binary.AppendUvarint(buf, uint64(len(e.Node)))
		buf = append(buf, e.Node...)
		buf = binary.AppendUvarint(buf, e.Counter)
	}

	return buf, nil
}

// Unmarshal parses the byte form into the receiver and validates
// the invariants.
func (v *VersionVector) Unmarshal(data []byte) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("versionvector: invalid count")
	}

	data = data[n:]

	if count > uint64(len(data)) {
		return errors.New("versionvector: count exceeds remaining data")
	}

	out := make([]crdt.Dot, 0, count)
	var prev string

	for k := range count {
		idLen, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("versionvector: invalid id length")
		}

		data = data[n:]

		if uint64(len(data)) < idLen {
			return errors.New("versionvector: id bytes truncated")
		}

		id := string(data[:idLen])

		data = data[idLen:]

		val, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("versionvector: invalid value")
		}

		data = data[n:]

		if val == 0 {
			return errors.New("versionvector: zero-valued entry violates invariant")
		}

		if k > 0 && id <= prev {
			return errors.New("versionvector: dots not sorted by id")
		}

		prev = id

		out = append(out, crdt.Dot{Node: id, Counter: val})
	}

	v.dots = out

	return nil
}
