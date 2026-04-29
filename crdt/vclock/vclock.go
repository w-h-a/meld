// Package vclock implements version vectors for causal ordering.
// No wall clocks. Monotonically increasing per node.
//
// References:
//   - Lamport, "Time, Clocks, and the Ordering of Events" (1978)
//   - DDIA chapter 5 (detecting concurrent writes)
package vclock
