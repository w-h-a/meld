package causal

import (
	"context"
	"time"

	"github.com/w-h-a/meld/antientropy"
	"github.com/w-h-a/meld/crdt"
)

type gcIntervalKey struct{}

// WithGCInterval sets how often the node sweeps acknowledged deltas out
// of its buffer.
func WithGCInterval[T crdt.Equatable[T]](d time.Duration) antientropy.Option[T] {
	return func(o *antientropy.Options[T]) {
		o.Context = context.WithValue(o.Context, gcIntervalKey{}, d)
	}
}

func gcIntervalFrom(ctx context.Context) time.Duration {
	if d, ok := ctx.Value(gcIntervalKey{}).(time.Duration); ok {
		return d
	}
	return 10 * time.Second
}
