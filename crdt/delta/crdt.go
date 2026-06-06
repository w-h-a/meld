// Package delta implements delta-state extensions for the CRDT catalog.
// Each base CRDT in meld supports a delta-mutator pair alongside its
// standard mutator. The anti-entropy package consumes them to ship
// delta-intervals instead of full state.
//
// References:
//   - Almeida, Shoker, Baquero, "Delta State Replicated Data Types" (2018)
//   - Shapiro et al., "A comprehensive study of Convergent and
//     Commutative Replicated Data Types" (2011), Section 2.4.2 on
//     state-based emulation of operation-based objects
package delta
