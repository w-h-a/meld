// Package orset implements the Observed-Remove Set CRDT.
// Add and remove are both supported without coordination.
// Used by flock for workload spec replication.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package orset
