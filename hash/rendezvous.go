// Package hash implements rendezvous hashing (highest random weight)
// for deterministic placement without coordination.
//
// Given the same set of nodes and a key, every caller computes the
// same assignment. Adding/removing a node only moves keys that were
// on that node.
//
// References:
//   - Thaler & Ravishankar, "Using Name-Based Mappings to Increase
//     Hit Rates" (1998)
package hash
