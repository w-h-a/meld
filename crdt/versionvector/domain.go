package versionvector

// Ordering is the result of Compare. The five values name the
// possible causal relationships between two version vectors. Names
// are written from the perspective of the receiver to Compare.
// So, for example, v.Compare(other) returns Greater when v dominates.
// ConcurrentGreater and ConcurrentLesser both mean the replicas
// diverged. Some counters favor v and some favor other; so, neither
// dominates.
// Consumers that only care about the partial order can treat both
// Concurrent values the same.
type Ordering int

const (
	Equal Ordering = iota
	Greater
	Lesser
	ConcurrentGreater
	ConcurrentLesser
)

type counter struct {
	id    string
	value uint64
}
