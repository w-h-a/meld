// Package crdt defines the common interface for conflict-free
// replicated data types. Each CRDT type implements Mergeable,
// guaranteeing convergence without coordination.
package crdt

// Mergeable is the core CRDT contract. Merge must be:
//   - Commutative: Merge(a, b) == Merge(b, a)
//   - Associative: Merge(Merge(a, b), c) == Merge(a, Merge(b, c))
//   - Idempotent: Merge(a, a) == a
type Mergeable[T any] interface {
	Merge(other T) T
}
