// Package orset implements the Observed-Remove Set CRDT.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package orset

import (
	"encoding/binary"
	"errors"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/versionvector"
)

// Make sure ORSet satisfies crdt.Mergeable. The witness uses a
// concrete instantiation because Go generics require one for a
// compile-time interface check.
var _ crdt.Mergeable[ORSet[struct{}]] = ORSet[struct{}]{}

// ORSet is the optimized state-based Observed-Remove Set. It holds
// a live G(row only)-Set of (element, nodeID, counter) triples and a
// version vector that records, for each nodeID, the highest counter
// ever observed at this ORSet.
type ORSet[T comparable] struct {
	live   map[triple[T]]struct{}
	vector versionvector.VersionVector
}

func New[T comparable]() ORSet[T] {
	return ORSet[T]{}
}

// Add returns a new Set with element added under a fresh unique
// triple. The triple's counter is the next counter for nodeID in
// the Set's version vector. The receiver is not modified. Callers
// pass their own node id and only their own.
//
// Re-adding an element that was previously removed mints a triple
// with a counter strictly greater than any counter ever observed
// for nodeID at this Set. So the new triple is observed as live
// after the next merge.
//
// Successive adds of the same element at the same nodeID are
// coalesced. Only the highest-counter triple survives in live
// because it subsumes the earlier ones. So local state stays
// bounded.
func (s ORSet[T]) Add(nodeID string, element T) ORSet[T] {
	nextV := s.vector.Increment(nodeID)
	counter := nextV.Get(nodeID)

	newLive := copyTriples(s.live, 1)
	newLive[triple[T]{element: element, nodeID: nodeID, counter: counter}] = struct{}{}
	newLive = coalesce(newLive)

	return ORSet[T]{live: newLive, vector: nextV}
}

// Remove returns a new Set with every (element, *, *) triple
// dropped from live. The version vector is not modified: the
// receiver has already observed every counter it has recorded, and
// dropping triples does not change what has been observed.
//
// The remove is observable to other replicas through Merge. A
// triple absent from live but covered by V (counter <= V[nodeID])
// is treated by Merge as removed and dropped from the union.
//
// The receiver is not modified.
func (s ORSet[T]) Remove(element T) ORSet[T] {
	newLive := make(map[triple[T]]struct{}, len(s.live))

	for t := range s.live {
		if t.element == element {
			continue
		}
		newLive[t] = struct{}{}
	}

	return ORSet[T]{live: newLive, vector: s.vector}
}

// Contains reports whether element appears in any live triple.
func (s ORSet[T]) Contains(element T) bool {
	for t := range s.live {
		if t.element == element {
			return true
		}
	}

	return false
}

// Elements returns the distinct live members in unspecified order.
func (s ORSet[T]) Elements() []T {
	seen := make(map[T]struct{})
	for t := range s.live {
		seen[t.element] = struct{}{}
	}

	out := make([]T, 0, len(seen))
	for e := range seen {
		out = append(out, e)
	}

	return out
}

// LiveCount returns the number of live triples the Set stores.
func (s ORSet[T]) LiveCount() int {
	return len(s.live)
}

// Clone returns a deep copy.
func (s ORSet[T]) Clone() ORSet[T] {
	return ORSet[T]{
		live:   copyTriples(s.live, 0),
		vector: s.vector.Clone(),
	}
}

// Merge combines two replicas' Sets into one. The question
// Merge has to answer is: when one side has a record and the
// other side does not, did the other side never see the record,
// or did it see the record and remove it?
//
// The version vector answers that question without a tombstone G-Set.
// Each side's V[n] is the highest counter at nodeID n that side has
// ever observed. So given a record (element, n, c) that one side
// has and the other does not, ask whether the other side's V[n]
// has reached c,
//
//	c <= other.V[n]	In this case, other has observed counter c
//		at n and is not storing the record. So it must have removed.
//		Drop it.
//
//	c > other.V[n]	In this case, other has never observed c
//		at n. The record is a concurrent add the other side has not
//		heard about yet. Keep.
//
// Worked example.
//
//	n1.Add("n1", "nginx")        V={n1:1}  live={(nginx, n1, 1)}
//	n2 := n1.Clone()             V={n1:1}  live={(nginx, n1, 1)}
//	n2.Remove("nginx")           V={n1:1}  live={}
//	n1.Add("n1", "nginx")        V={n1:2}  live={(nginx, n1, 2)}
//
// n1's second add coalesced (nginx, n1, 1) into (nginx, n1, 2).
// Merging n1 with n2, the only triple to classify is
// (nginx, n1, 2). It is not in n2.live. n2.V[n1] = 1 and the
// triple's counter is 2, so c > V[n]. n2 has never observed
// counter 2 at n1, so it could not have removed it. Keep.
// Contains("nginx") is true after merge. The concurrent add wins.
func (s ORSet[T]) Merge(other ORSet[T]) ORSet[T] {
	mergedV := s.vector.Merge(other.vector)
	union := make(map[triple[T]]struct{}, len(s.live)+len(other.live))

	// Walk s.live. Records in both sides go straight
	// in. Records in s but not in other get the V question.
	for t := range s.live {
		if _, ok := other.live[t]; ok {
			union[t] = struct{}{}
			continue
		}
		if t.counter > other.vector.Get(t.nodeID) {
			// other has never observed this counter at this
			// nodeID; so, keep.
			union[t] = struct{}{}
		}
		// Otherwise, other observed this counter and dropped the
		// record; so, drop it.
	}

	// Walk other.live. Records present in s.live
	// were already handled above, so skip. Records in
	// other but not in s get the V question.
	for t := range other.live {
		if _, ok := s.live[t]; ok {
			continue
		}
		if t.counter > s.vector.Get(t.nodeID) {
			// s has never observed this counter at this
			// nodeID; so, keep.
			union[t] = struct{}{}
		}
		// Otherwise, s observed this counter and dropped the
		// record; so, drop it.
	}

	return ORSet[T]{live: coalesce(union), vector: mergedV}
}

// Marshal encodes the Set for persistence or the wire. T is opaque
// to the Set, so the caller supplies the element codec. The wire
// format is non-canonical because map iteration order is not
// stable. So two Marshal calls on the same Set may produce
// different bytes that decode to equivalent states.
//
// Wire format:
//
//	uvarint(vectorLen), vectorBytes  (versionvector.Marshal)
//	uvarint(liveCount), then per triple:
//	    uvarint(elementLen), elementBytes
//	    uvarint(nodeIDLen),  nodeIDBytes
//	    uvarint(counter)
//
// encodeElement must not be nil.
func (s ORSet[T]) Marshal(encodeElement func(T) ([]byte, error)) ([]byte, error) {
	vectorBytes, err := s.vector.Marshal()
	if err != nil {
		return nil, err
	}

	buf := binary.AppendUvarint(make([]byte, 0), uint64(len(vectorBytes)))
	buf = append(buf, vectorBytes...)

	buf = binary.AppendUvarint(buf, uint64(len(s.live)))
	for t := range s.live {
		encoded, err := encodeTriple(t, encodeElement)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}

	return buf, nil
}

// Unmarshal parses the byte form into the receiver. The caller
// supplies the element decoder, matching the encoder used at
// Marshal. decodeElement must not be nil.
func (s *ORSet[T]) Unmarshal(data []byte, decodeElement func([]byte) (T, error)) error {
	vectorLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("orset: invalid vector length")
	}

	data = data[n:]

	if uint64(len(data)) < vectorLen {
		return errors.New("orset: vector bytes truncated")
	}

	var vector versionvector.VersionVector
	if err := vector.Unmarshal(data[:vectorLen]); err != nil {
		return err
	}

	data = data[vectorLen:]

	liveCount, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("orset: invalid live count")
	}

	data = data[n:]

	if liveCount > uint64(len(data)) {
		return errors.New("orset: live count exceeds remaining data size")
	}

	live := make(map[triple[T]]struct{}, liveCount)
	for range liveCount {
		t, rest, err := decodeTriple[T](data, decodeElement)
		if err != nil {
			return err
		}
		live[t] = struct{}{}
		data = rest
	}

	s.live = live
	s.vector = vector

	return nil
}
