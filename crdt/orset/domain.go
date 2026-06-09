package orset

// triple is the per-add record in the optimized OR-Set.
type triple[T comparable] struct {
	element T
	nodeID  string
	counter uint64
}

// addKey identifies an add by its (element, nodeID) pair.
type addKey[T comparable] struct {
	element T
	nodeID  string
}
