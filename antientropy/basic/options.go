package basic

import (
	"context"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/crdt"
)

// transitiveKey is the private context key transitive mode is stashed under.
type transitiveKey struct{}

// WithTransitive turns on transitive mode: deltas received from one peer
// are re-buffered and forwarded to others.
func WithTransitive[T crdt.Mergeable[T]]() antientropy.Option[T] {
	return func(o *antientropy.Options[T]) {
		o.Context = context.WithValue(o.Context, transitiveKey{}, true)
	}
}

// transitiveFrom reads the transitive flag.
func transitiveFrom(ctx context.Context) bool {
	v, _ := ctx.Value(transitiveKey{}).(bool)
	return v
}
