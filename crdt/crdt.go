// Package crdt defines the common interface for conflict-free
// replicated data types. Each CRDT type implements Mergeable,
// guaranteeing convergence without coordination.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011), Section 2.4.2 on
//     state-based emulation of operation-based objects
//   - Almeida, Shoker, Baquero, "Delta State Replicated Data
//     Types" (2018)
package crdt

// Dot uniquely names one event from one node: which node produced it, and that
// node's own running count of events.
type Dot struct {
	Node    string
	Counter uint64
}

// Mergeable is the core CRDT contract. Merge must be:
//   - Commutative: Merge(a, b) == Merge(b, a)
//   - Associative: Merge(Merge(a, b), c) == Merge(a, Merge(b, c))
//   - Idempotent: Merge(a, a) == a
type Mergeable[T any] interface {
	Merge(other T) T
}
