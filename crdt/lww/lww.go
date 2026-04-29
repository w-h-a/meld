// Package lww implements the Last-Writer-Wins Register CRDT.
// Uses version vectors (not wall clocks) for ordering.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package lww
