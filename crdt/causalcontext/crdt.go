// Package causalcontext tracks the set of dots a replica has observed.
package causalcontext

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/versionvector"
)

// Make sure CausalContext satisfies crdt.Mergeable.
var _ crdt.Mergeable[CausalContext] = CausalContext{}

// CausalContext is the set of dots a replica has seen, in two parts.
// normals counts each node up from 1, with no holes. {n1: 3} means the replica
// has seen n1's dots 1, 2, and 3.
// exceptions holds dots the replica has seen but cannot count yet because a
// lower-numbered dot from the same node is missing.
type CausalContext struct {
	normals    versionvector.VersionVector
	exceptions []crdt.Dot
}

func New() CausalContext {
	return CausalContext{}
}

// Next returns the counter that the node should use for its next dot.
// It only reads. A node sees its own dots in order, so it never
// holds an exception for itself. Callers pass their own node id and only
// their own.
func (c CausalContext) Next(node string) uint64 {
	return c.normals.Get(node) + 1
}

// Contains reports whether the replica has seen the dot.
// normals.Get(node) is the highest dot from the node with no gaps. If it is
// 3, then 1, 2, and 3 have been seen. So counter <= normals.Get(node) means the
// dot is one of those and is seen. Otherwise, it is seen only if it is in
// exceptions.
func (c CausalContext) Contains(node string, counter uint64) bool {
	if counter <= c.normals.Get(node) {
		return true
	}

	for _, e := range c.exceptions {
		if e.Node == node && e.Counter == counter {
			return true
		}
	}

	return false
}

// Clone returns a deep copy.
func (c CausalContext) Clone() CausalContext {
	out := CausalContext{normals: c.normals.Clone()}

	if len(c.exceptions) > 0 {
		out.exceptions = make([]crdt.Dot, 0, len(c.exceptions))
		out.exceptions = append(out.exceptions, c.exceptions...)
	}

	return out
}

// Observe records that the replica has seen the dot and returns the updated
// context. The receiver is not mutated.
//   - If the dot has already been seen, nothing changes.
//   - If a lower dot from the node is missing, it goes into exceptions.
//   - If it is the next dot in order, it extends normals and any exceptions
//     that are now consecutive get folded in to normals.
func (c CausalContext) Observe(node string, counter uint64) CausalContext {
	if c.Contains(node, counter) {
		return c
	}

	// If my highest count for this node + 1 is less than the counter,
	// append to exceptions.
	if counter > c.normals.Get(node)+1 {
		exceptions := make([]crdt.Dot, 0, len(c.exceptions)+1)
		exceptions = append(exceptions, c.exceptions...)
		exceptions = append(exceptions, crdt.Dot{Node: node, Counter: counter})

		return CausalContext{normals: c.normals, exceptions: exceptions}
	}

	// We know it's not an exception, so now it's time to extend
	// normals while we can. For example, suppose normals {n1: 2} and
	// exceptions {(n1, 4), (n1, 5), (n1, 6)}. Now Observe(n1, 3). After
	// only 1 increment, normals is {n1: 3}, but we've already
	// seen events 4, 5, and 6 from n1 so we have to continue
	// incrementing normals.
	normals := c.normals.Increment(node)
	for c.Contains(node, normals.Get(node)+1) {
		normals = normals.Increment(node)
	}

	// Now it's time to make a new exceptions. Loop over existing
	// exceptions and append to new exceptions only if we're dealing
	// with a different node or it's this node but there are still gaps.
	exceptions := make([]crdt.Dot, 0, len(c.exceptions))
	for _, e := range c.exceptions {
		if e.Node == node && e.Counter <= normals.Get(node) {
			continue
		}
		exceptions = append(exceptions, e)
	}

	return CausalContext{normals: normals, exceptions: exceptions}
}

// Merge returns a causal context holding every dot either side has seen.
// It takes the larger count per node from the two normals, then observes
// every exception from both sides into the result, folding each one in or
// keeping it as gaps dictate.
//
// Worked example.
//
//	c = normals {n1: 2}, exceptions {(n1, 4)}	seen n1's 1, 2, and 4
//	other = normals {n1: 3}, exceptions {}	seen n1's 1, 2, and 3
//	merged = normals {n1: 4}, exceptions {}		seen n1's 1, 2, 3, and 4
//
// Merge takes the larger normals, {n1: 3}, then observes c's exception (n1, 4).
// With 3 now in place, 4 is consecutive and folds in to normals, giving {n1: 4}.
//
// Merge is commutative, associative, and idempotent because it computes the
// union of two sets of dots. So message order, grouping, and duplicates do not
// change the result.
func (c CausalContext) Merge(other CausalContext) CausalContext {
	merged := CausalContext{normals: c.normals.Merge(other.normals)}

	for _, e := range c.exceptions {
		merged = merged.Observe(e.Node, e.Counter)
	}

	for _, e := range other.exceptions {
		merged = merged.Observe(e.Node, e.Counter)
	}

	return merged
}

// Marshal encodes the context for persistence or the wire. The normals bytes are
// length-prefixed so Unmarshal can split them from the exceptions. Exceptions are
// sorted by node then counter here, because they are stored in append order, so
// the encoding is canonical: two contexts that have seen the same dots marshal to
// identical bytes regardless of the order their exceptions arrived.
//
// Format: uvarint(len(normalsBytes)), normalsBytes, uvarint(exceptionCount),
// then per exception uvarint(nodeLen), nodeBytes, uvarint(counter).
func (c CausalContext) Marshal() ([]byte, error) {
	normalsBytes, err := c.normals.Marshal()
	if err != nil {
		return nil, err
	}

	buf := binary.AppendUvarint(make([]byte, 0), uint64(len(normalsBytes)))
	buf = append(buf, normalsBytes...)

	sorted := make([]crdt.Dot, len(c.exceptions))
	copy(sorted, c.exceptions)
	slices.SortFunc(sorted, crdt.Dot.Compare)

	buf = binary.AppendUvarint(buf, uint64(len(sorted)))
	for _, e := range sorted {
		buf = binary.AppendUvarint(buf, uint64(len(e.Node)))
		buf = append(buf, e.Node...)
		buf = binary.AppendUvarint(buf, e.Counter)
	}

	return buf, nil
}

// Unmarshal parses the byte form into the receiver and validates the canonical
// form: the normals are a valid version vector, and the exceptions are sorted by
// node then counter, strictly increasing, and each strictly above normals+1 for
// its node. A dot at or below normals+1 would have been counted or folded into
// normals, so it has no place as an exception.
func (c *CausalContext) Unmarshal(data []byte) error {
	normalsLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("causalcontext: invalid normals length")
	}
	data = data[n:]

	if uint64(len(data)) < normalsLen {
		return errors.New("causalcontext: normals bytes truncated")
	}

	var normals versionvector.VersionVector
	if err := normals.Unmarshal(data[:normalsLen]); err != nil {
		return err
	}
	data = data[normalsLen:]

	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("causalcontext: invalid exception count")
	}
	data = data[n:]

	if count > uint64(len(data)) {
		return errors.New("causalcontext: exception count exceeds remaining data")
	}

	exceptions := make([]crdt.Dot, 0, count)
	var prev crdt.Dot

	for k := range count {
		nodeLen, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("causalcontext: invalid node length")
		}
		data = data[n:]

		if uint64(len(data)) < nodeLen {
			return errors.New("causalcontext: node bytes truncated")
		}
		node := string(data[:nodeLen])
		data = data[nodeLen:]

		counter, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("causalcontext: invalid counter")
		}
		data = data[n:]

		if counter <= normals.Get(node)+1 {
			return errors.New("causalcontext: exception at or below normals+1")
		}

		e := crdt.Dot{Node: node, Counter: counter}
		if k > 0 && e.Compare(prev) <= 0 {
			return errors.New("causalcontext: exceptions not sorted")
		}
		prev = e

		exceptions = append(exceptions, e)
	}

	c.normals = normals
	c.exceptions = exceptions

	return nil
}
