package causal

import (
	"context"
	"sync"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/crdt"
	"go.opentelemetry.io/otel/trace"
)

// causalReplicator is the causal anti-entropy replicator for a CRDT. It
// ships only the contiguous run of deltas that neighbors have not yet
// acknowledged.
//
// state and seq are durable. Suppose the node has produced an shipped
// neighbor j its deltas 0 through 4, so seq is 5 and j has acked through 5.
// The node crashes. If seq restarts at 0, the next local writes reuse
// seq 0, 1, 2, etc. A late ack for the pre-crash deltas then records j
// as caught up through 5, so the node believes j already holds everything
// below 5 and never ships new deltas to j.
//
// deltas and acks are rebuildable. A restart starts them empty. With no
// acks recorded, the node assumes every neighbor is behind, and with no
// buffered deltas, it ships full state. Neighboros converge and re-ack,
// which refills acks and lets the node resume sending incremental deltas.
// Losing deltas or acks therefore costs only some redundant full-state
// sends, never correctness.
type causalReplicator[T crdt.Mergeable[T]] struct {
	options    antientropy.Options[T]
	gcInterval time.Duration

	mtx    sync.RWMutex
	state  T                 // durable
	seq    uint64            // durable
	deltas T                 // volatile
	acks   map[string]uint64 // volatile

	cancel context.CancelFunc
	wg     sync.WaitGroup

	tracer trace.Tracer
}
