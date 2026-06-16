// Package antientropy replicates CRDTs toward convergence by gossiping
// deltas. It drives any crdt.Mergeable that has delta-mutators through
// a gossip transport and a membership source.
package antientropy

import (
	"context"

	"github.com/w-h-a/meld/crdt"
)

// Replicator runs anti-entropy for a delta supporting CRDT. The app submits
// the deltas its local operations produce, and the Replicator gossips them
// to peers and merges what it receives, so every node converges.
type Replicator[T crdt.Mergeable[T]] interface {
	// Submit feeds a locally produces delta into the node. The node joins
	// it into the state and queues it for the next gossip round.
	Submit(delta T)

	// State returns the current converged state.
	State() T

	// Start launches the receive and gossip loops, returning once they
	// are running.
	Start(ctx context.Context) error

	// Stop halts the loops.
	Stop(ctx context.Context) error
}
