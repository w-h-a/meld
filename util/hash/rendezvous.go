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

import "hash/fnv"

// Assign returns the node with the highest rendezvous hash score
// for the given key.
func Assign(nodes []string, key string) string {
	var best string
	var bestScore uint64

	for i, node := range nodes {
		score := score(node, key)
		if i == 0 || score > bestScore {
			best = node
			bestScore = score
		}
	}

	return best
}

func score(node, key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(node))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return h.Sum64()
}
