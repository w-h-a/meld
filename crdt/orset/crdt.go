// Package orset implements the optimized Observed-Remove Set CRDT.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package orset

import (
	"encoding/binary"
	"errors"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/causalcontext"
)

// Make sure ORSet satisfies crdt.Equatable. The witness uses a
// concrete instantiation because Go generics require one for a
// compile-time interface check.
var _ crdt.Equatable[ORSet[struct{}]] = ORSet[struct{}]{}

// ORSet is the optimized state-based Observed-Remove Set. It holds a
// live G(row only)-Set of (element, node, counter) triples and a
// causal context that records which dots this ORSet has observed
type ORSet[T comparable] struct {
	live map[triple[T]]struct{}
	seen causalcontext.CausalContext
}

func New[T comparable]() ORSet[T] {
	return ORSet[T]{}
}

// Add returns a new Set with element added under a fresh unique
// triple. The triple's counter is the next counter for nodeID in
// the Set's causal context. The receiver is not modified. Callers
// pass their own node id and only their own.
//
// Re-adding an element that was previously removed mints a triple
// with a counter strictly greater than any counter ever observed
// for nodeID at this Set.
//
// Successive adds of the same element at the same nodeID are
// coalesced. Only the highest-counter triple survives in live
// because it subsumes the earlier ones. So local state stays
// bounded.
func (s ORSet[T]) Add(nodeID string, element T) ORSet[T] {
	counter := s.seen.Next(nodeID)
	newSeen := s.seen.Observe(nodeID, counter)

	newLive := copyTriples(s.live, 1)
	newLive[triple[T]{element: element, dot: crdt.Dot{Node: nodeID, Counter: counter}}] = struct{}{}
	newLive = coalesce(newLive)

	return ORSet[T]{live: newLive, seen: newSeen}
}

// AddDelta returns the delta for adding element at nodeID: So,
// we need it's both in s.live and observed. The receiver is
// not modfied, and callers pass their own node id and only their own.
//
//	s.Merge(s.AddDelta(n, e)) == s.Add(n, e)
func (s ORSet[T]) AddDelta(nodeID string, element T) ORSet[T] {
	counter := s.seen.Next(nodeID)
	dot := crdt.Dot{Node: nodeID, Counter: counter}

	live := map[triple[T]]struct{}{
		{element: element, dot: dot}: {},
	}

	seen := causalcontext.New().Observe(nodeID, counter)

	return ORSet[T]{live: live, seen: seen}
}

// Remove returns a new Set with every (element, *, *) triple
// dropped from live. The causal context is not modified: the
// receiver has already observed every counter it has recorded, and
// dropping triples does not change what has been observed. The
// receiver is not modified.
func (s ORSet[T]) Remove(element T) ORSet[T] {
	newLive := make(map[triple[T]]struct{}, len(s.live))

	for t := range s.live {
		if t.element == element {
			continue
		}
		newLive[t] = struct{}{}
	}

	return ORSet[T]{live: newLive, seen: s.seen}
}

// RemoveDelta returns the delta for removing element: So, we need
// to ensure it's not in s.live but seen. The receiver is not
// modified.
//
//	s.Merge(s.RemoveDelta(e)) == s.Remove(e)
func (s ORSet[T]) RemoveDelta(element T) ORSet[T] {
	seen := causalcontext.New()

	for t := range s.live {
		if t.element != element {
			continue
		}
		seen = seen.Observe(t.dot.Node, t.dot.Counter)
	}

	return ORSet[T]{seen: seen}
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
		live: copyTriples(s.live, 0),
		seen: s.seen.Clone(),
	}
}

// Equal reports whether s and other hold the same live triples and have
// observed the same dots.
func (s ORSet[T]) Equal(other ORSet[T]) bool {
	if len(s.live) != len(other.live) {
		return false
	}

	for t := range s.live {
		if _, ok := other.live[t]; !ok {
			return false
		}
	}

	return s.seen.Equal(other.seen)
}

// Merge combines two replicas' Sets into one. The question Merge has
// to answer is: when one side has a record and the other side does
// not, did the other side never see the record, or did it see the
// record and remove it?
//
// The causal context answers that question without a tombstone G-Set.
// Each side's context records every dot it has observed. So given a
// record (element, n, c) that one side has and the other does not,
// ask whether the other side has seen the dot (n, c):
//
//	not in other.live and other has NOT seen (n, c). Keep.
//
//	not in other.live and other has seen (n, c), so other must
//		have removed it. Drop it.
//
// Worked example.
//
//	n1.Add("n1", "nginx")        seen={n1: 1}  live={(nginx, n1, 1)}
//	n2 := n1.Clone()             seen={n1: 1}  live={(nginx, n1, 1)}
//	n2.Remove("nginx")           seen={n1: 1}  live={}
//	n1.Add("n1", "nginx")        seen={n1: 2}  live={(nginx, n1, 2)}
//
// n1's second add coalesced (nginx, n1, 1) into (nginx, n1, 2).
// Merging n1 with n2, the only triple to classify is (nginx, n1, 2).
// It is not in n2.live. n2 has seen n1 only up to 1, so it has not
// seen the dot (n1, 2). It could not have removed it. Keep. The
// concurrent add wins.
func (s ORSet[T]) Merge(other ORSet[T]) ORSet[T] {
	mergedSeen := s.seen.Merge(other.seen)
	union := make(map[triple[T]]struct{}, len(s.live)+len(other.live))

	// Walk s.live. Records in both sides go straight in. Records in
	// s but not in other get the seen question.
	for t := range s.live {
		if _, ok := other.live[t]; ok {
			union[t] = struct{}{}
			continue
		}
		if !other.seen.Contains(t.dot.Node, t.dot.Counter) {
			// other has never seen this dot, so keep.
			union[t] = struct{}{}
		}
		// Otherwise, other saw this dot and dropped, so
		// drop it.
	}

	// Walk other.live. Records present in s.live were already
	// handled above, so skip. Records in other but not in s get
	// the seen question.
	for t := range other.live {
		if _, ok := s.live[t]; ok {
			continue
		}
		if !s.seen.Contains(t.dot.Node, t.dot.Counter) {
			// s has never seen this dot, so keep.
			union[t] = struct{}{}
		}
		// Otherwise, s saw this dot and dropped, so
		// drop it.
	}

	return ORSet[T]{live: coalesce(union), seen: mergedSeen}
}

// Marshal encodes the Set for persistence or the wire. T is opaque
// to the Set, so the caller supplies the element codec. The wire
// format is non-canonical because map iteration order is not
// stable. So two Marshal calls on the same Set may produce
// different bytes that decode to equivalent states.
//
// Wire format:
//
//	uvarint(seenLen), seenBytes  (causalcontext.Marshal)
//	uvarint(liveCount), then per triple:
//	    uvarint(elementLen), elementBytes
//	    uvarint(nodeIDLen),  nodeIDBytes
//	    uvarint(counter)
//
// encodeElement must not be nil.
func (s ORSet[T]) Marshal(encodeElement func(T) ([]byte, error)) ([]byte, error) {
	seenBytes, err := s.seen.Marshal()
	if err != nil {
		return nil, err
	}

	buf := binary.AppendUvarint(make([]byte, 0), uint64(len(seenBytes)))
	buf = append(buf, seenBytes...)

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
	seenLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("orset: invalid context length")
	}

	data = data[n:]

	if uint64(len(data)) < seenLen {
		return errors.New("orset: context bytes truncated")
	}

	var seen causalcontext.CausalContext
	if err := seen.Unmarshal(data[:seenLen]); err != nil {
		return err
	}

	data = data[seenLen:]

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
	s.seen = seen

	return nil
}
