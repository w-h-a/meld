// Package pncounter implements the Positive-Negative Counter CRDT.
// Composes two G-Counters: one for increments, one for decrements.
// Value is the difference.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package pncounter

import (
	"encoding/binary"
	"errors"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/crdt/gcounter"
)

// Make sure PNCounter satisfies crdt.Mergeable.
var _ crdt.Mergeable[PNCounter] = PNCounter{}

// PNCounter is a counter that goes both up and down without
// coordination. It holds two G-Counters. P counts increments and N
// counts decrements.
type PNCounter struct {
	p gcounter.GCounter
	n gcounter.GCounter
}

func New() PNCounter {
	return PNCounter{p: gcounter.New(), n: gcounter.New()}
}

// Value returns the counter's reading, the total increments minus the
// total decrements.
func (pn PNCounter) Value() int64 {
	return int64(pn.p.Value()) - int64(pn.n.Value())
}

// Increment returns a new counter with nodeID's increment slot raised by
// 1. The receiver is not modified. So counters are safe to share across
// routines and the wire. Callers pass their own node id and only their own.
func (pn PNCounter) Increment(nodeID string) PNCounter {
	return PNCounter{p: pn.p.Increment(nodeID), n: pn.n}
}

// Decrement returns a new counter with nodeID's decrement slot raised by
// 1. A decrement raises the N counter. It does not lower the P counter.
func (pn PNCounter) Decrement(nodeID string) PNCounter {
	return PNCounter{p: pn.p, n: pn.n.Increment(nodeID)}
}

// Clone returns a deep copy.
func (pn PNCounter) Clone() PNCounter {
	return PNCounter{p: pn.p.Clone(), n: pn.n.Clone()}
}

// Merge merges the two underlying G-Counters independently. P merges with
// P and N merges with N. Each G-Counter converges on its own through
// element-wise max. The reading is a pure function of the two converged
// values. So the PN-Counter converges too.
//
// Worked example.
//
//	n1 increments 3 times and decrements once.
//	    P = {n1: 3}, N = {n1: 1}, value 2
//	n2 increments once and decrements twice.
//	    P = {n2: 1}, N = {n2: 2}, value -1
//	merged.
//	    P = {n1: 3, n2: 1}, N = {n1: 1, n2: 2}, value 4 - 3 = 1
//
// Merge is commutative, associative, and idempotent because each
// underlying G-Counter merge is. So message order, grouping, and
// duplicates do not change the result.
func (pn PNCounter) Merge(other PNCounter) PNCounter {
	return PNCounter{
		p: pn.p.Merge(other.p),
		n: pn.n.Merge(other.n),
	}
}

// Marshal encodes the counter for persistence or the wire. It delegates
// to the two underlying G-Counters and frames the P bytes with a length
// prefix so Unmarshal can split the two halves. The N bytes are the
// remainder.
//
// Format: uvarint(len(pBytes)), pBytes, nBytes.
//
// The encoding is canonical because each G-Counter marshals canonically
// and the two halves appear in a fixed order. So two counters in the same
// state marshal to identical bytes, and the anti-entropy layer can compare
// or hash wire bytes to detect divergence.
func (pn PNCounter) Marshal() ([]byte, error) {
	pBytes, err := pn.p.Marshal()
	if err != nil {
		return nil, err
	}

	nBytes, err := pn.n.Marshal()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, binary.MaxVarintLen64+len(pBytes)+len(nBytes))
	buf = binary.AppendUvarint(buf, uint64(len(pBytes)))
	buf = append(buf, pBytes...)
	buf = append(buf, nBytes...)

	return buf, nil
}

// Unmarshal parses the byte form into the receiver. It reads the P length
// prefix, hands the P bytes to one G-Counter and the remainder to the
// other. Each G-Counter validates its own invariants, and those errors
// surface here unchanged.
func (pn *PNCounter) Unmarshal(data []byte) error {
	pLen, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("pncounter: invalid P length")
	}

	data = data[n:]

	if uint64(len(data)) < pLen {
		return errors.New("pncounter: P bytes truncated")
	}

	var pCounter gcounter.GCounter
	if err := pCounter.Unmarshal(data[:pLen]); err != nil {
		return err
	}

	var nCounter gcounter.GCounter
	if err := nCounter.Unmarshal(data[pLen:]); err != nil {
		return err
	}

	pn.p = pCounter
	pn.n = nCounter

	return nil
}
