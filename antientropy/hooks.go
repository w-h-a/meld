package antientropy

import (
	"context"

	"github.com/w-h-a/meld/crdt"
)

// OnReceive is an optional hook the node calls after each merge.
type OnReceive[T crdt.Mergeable[T]] func(ctx context.Context, before, after T)

// OnSend is an optional hook the node calls before each gossip round.
type OnSend[T crdt.Mergeable[T]] func(ctx context.Context, state T)
