package gcounter

// slot is one node's entry in a GCounter.
type slot struct {
	id    string
	value uint64
}
