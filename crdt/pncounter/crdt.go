// Package pncounter implements the Positive-Negative Counter CRDT.
// Composes two G-Counters: one for increments, one for decrements.
// Value is the difference.
//
// References:
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011)
package pncounter
