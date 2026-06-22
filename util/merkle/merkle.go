// Package merkle builds Merkle trees so two replicas can find the few keys
// they disagree on.
//
// A Merkle tree is a tree of hashes. Each leaf hashes a chunk of the data,
// and each parent hashes its children, so the root is one hash that
// depends on everything below it. The point is cheap comparison: if two
// subtrees have equal roots, they almost certainly hold identical data;
// otherwise, we compare child hashes and follow only the ones that differ.
// As a consequence, entire matching regions are skipped.
//
// Each key is hashed to a number, the numbers are split into equal buckets,
// and each bucket is a leaf. Two replicas build the same buckets from
// whatever keys they each hold, so a key on only one side still lands in a
// bucket and shows up as a mismatch.
package merkle

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"math"
	"slices"
)

const (
	// bucketCount is how many buckets the tree sorts keys into. Each key is
	// hashed to a number that picks its bucket, and every bucket is a leaf of
	// the tree whose hash summarizes the keys in it.
	//
	// Example with bucketCount = 4. Three keys hash into buckets:
	//
	//	"bob" -> bucket 0
	// 	"alice" -> bucket 2
	//	"carol" -> bucket 2
	//
	// So buckets 1 and 3 are empty. When two replicas compare trees and only
	// bucket 2's hash differs, the disagreement is narrowed to {alice, carol}
	// instead of every key.
	//
	// Must be a power of two for a balanced tree. Hardcoded for now.
	bucketCount = 4096

	// bucketWidth is the length of one segment: the whole span of hash numbers
	// divided by bucketCount. There are 2^64 possible numbers (0 through
	// MaxUint64). So each segment is that total over bucketCount. We add 1
	// since MaxUint64/bucketCount rounds down.
	bucketWidth = math.MaxUint64/bucketCount + 1

	// The whole tree lives in one flat array. Instead of each node pointing to
	// its children, a node's index finds them by arithmetic: the node at index
	// i has children at 2i+1 and 2i+2, and the root is index 0.
	//
	// Example with bucketCount = 4: 4 leaves plus 3 internal nodes, 7 total,
	// in the array [0 1 2 3 4 5 6]:
	//
	//          0          index 0: root
	//         / \
	//        1   2        indexes 1-2: internal nodes
	//       / \ / \
	//      3 4 5 6        indexes 3-6: the 4 leaf buckets
	//
	// Node 0's children are at indexes 1 and 2; node 1's are at 3 and 4.
	// Leaves begin at index 3 = firstLeaf (bucketCount - 1), and the array
	// holds 7 = 2*bucketCount - 1 nodes.
	nodeCount = 2*bucketCount - 1
	firstLeaf = bucketCount - 1
)

// Pair is one key and the hash of its value.
type Pair struct {
	Key  string
	Hash []byte
}

// Range is a span of hash numbers [Lo, Hi], bounds inclusive, that two
// trees disagree on.
type Range struct {
	Lo uint64
	Hi uint64
}

// Tree is the Merkle tree stored as a flat array laid out in order
// such that node i's children are at 2i+1 and 2i+2.
type Tree struct {
	nodes []uint64
}

// Token hashes a key.
func Token(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// Build constructs the tree from the pairs.
func Build(pairs []Pair) Tree {
	// Drop each pair into its bucket.
	buckets := make([][]Pair, bucketCount)
	for _, p := range pairs {
		b := int(Token(p.Key) / bucketWidth)
		buckets[b] = append(buckets[b], p)
	}

	nodes := make([]uint64, nodeCount)

	// Fill the bottom row: each leaf is the hash of one bucket's contents.
	// Bucket b's leaf sits at firstLeaf+b
	for b, bucket := range buckets {
		nodes[firstLeaf+b] = bucketHash(bucket)
	}

	// Fill every node above the leaves: each is the hash of its two
	// children.
	for i := firstLeaf - 1; i >= 0; i-- {
		nodes[i] = combine(nodes[2*i+1], nodes[2*i+2])
	}

	return Tree{nodes: nodes}
}

// bucketHash folds a bucket's pairs into a single number.
func bucketHash(pairs []Pair) uint64 {
	slices.SortFunc(pairs, func(a, b Pair) int {
		return cmp.Compare(a.Key, b.Key)
	})

	h := fnv.New64a()
	for _, p := range pairs {
		h.Write([]byte(p.Key))
		h.Write([]byte{0})
		h.Write(p.Hash)
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// combine hashes a node's two child hashes into the node's own hash.
func combine(left, right uint64) uint64 {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], left)
	binary.BigEndian.PutUint64(buf[8:16], right)

	h := fnv.New64a()
	h.Write(buf[:])
	return h.Sum64()
}

// Diff returns the hash-number ranges where two trees disagree, in
// ascending order, merging neighboring divergent buckets into one range.
func Diff(a, b Tree) []Range {
	var ranges []Range
	diffNode(a, b, 0, &ranges)
	return ranges
}

// diffNode compares node i in the two trees.
//   - Equal hashes: the subtrees are identical, so there is nothing to
//     report below. Return.
//   - A leaf reached here: its bucket's hash differs, so record the
//     bucket's range.
//   - An internal node reached here: the difference is somewhere below, so
//     recurse into both children.
func diffNode(a, b Tree, i int, ranges *[]Range) {
	if a.nodes[i] == b.nodes[i] {
		return
	}

	if i >= firstLeaf {
		appendRange(ranges, i-firstLeaf)
		return
	}

	diffNode(a, b, 2*i+1, ranges)
	diffNode(a, b, 2*i+2, ranges)
}

// appendRange records bucket b's number range, merging it with the
// previous one when they touch. Bucket b starts at lo = b*bucketWidth and
// is bucketWidth number long, so it ends at hi = lo + bucketWidth - 1. If
// low is exactly one past the previous range's Hi, the two are contiguous
// and are extended into one; otherwise a new range is added.
//
// Example with bucketWidth = 25 and buckets 2 and 3 both divergent: bucket
// 2 is [50, 74], recorded as is; bucket 3 is [75, 99], and 75 == 74+1, so it
// extends the previous range to [50,99] instead of adding a second.
func appendRange(ranges *[]Range, b int) {
	lo := uint64(b) * bucketWidth
	hi := lo + bucketWidth - 1

	if n := len(*ranges); n > 0 && (*ranges)[n-1].Hi+1 == lo {
		(*ranges)[n-1].Hi = hi
		return
	}

	*ranges = append(*ranges, Range{Lo: lo, Hi: hi})
}
