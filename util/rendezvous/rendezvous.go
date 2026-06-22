// Package rendezvous implements rendezvous hashing (highest random weight)
// for deterministic placement without coordination.
//
// Given the same set of nodes and a key, every caller computes the
// same assignment. Adding/removing a node only moves keys that were
// on that node.
//
// References:
//   - Thaler & Ravishankar, "Using Name-Based Mappings to Increase
//     Hit Rates" (1998)
package rendezvous

import (
	"cmp"
	"hash/fnv"
	"slices"
)

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

// AssignN returns the n nodes with the highest rendezvous scores for the
// given key, ordered from highest score to lowest. This is the key's
// preference list: the first node is the primary, the rest are the
// next-best holders in order. If n is at least len(nodes), every node is
// returned, still ranked. If n <= 0 or nodes is empty, the result is nil.
//
// Like Assign, the result depends only on the node set and the key, not
// the order the nodes are passed in, so every caller computes the same
// list. A score tie breaks on the node id, which preserves that property
// even in the astronomically unlikely case of equal scores. Adding or
// removing a node only changes the keys that node moves into or out of
// the top n for, so disruption on a membership change is minimal.
func AssignN(nodes []string, key string, n int) []string {
	if n <= 0 || len(nodes) == 0 {
		return nil
	}

	scored := make([]scoredNode, len(nodes))
	for i, node := range nodes {
		scored[i] = scoredNode{node: node, score: score(node, key)}
	}

	slices.SortFunc(scored, func(a, b scoredNode) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		return cmp.Compare(b.node, a.node)
	})

	if n > len(scored) {
		n = len(scored)
	}

	out := make([]string, n)
	for i := range out {
		out[i] = scored[i].node
	}

	return out
}

func score(node, key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(node))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return h.Sum64()
}

type scoredNode struct {
	node  string
	score uint64
}
