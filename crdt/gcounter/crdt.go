// Package gcounter implements the Grow-only Counter CRDT.
// Each node increments its own slot. Value is the sum.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package gcounter
