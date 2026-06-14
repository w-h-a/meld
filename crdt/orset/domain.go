package orset

import "github.com/w-h-a/meld/crdt"

// triple is the per-add record in the optimized OR-Set.
// It is (element, node, counter), where (node, counter)
// is a dot.
type triple[T comparable] struct {
	element T
	dot     crdt.Dot
}

// addKey identifies an add by its (element, nodeID) pair.
type addKey[T comparable] struct {
	element T
	node    string
}
