package memory

import (
	"context"

	"github.com/w-h-a/meld/gossip"
)

type (
	networkKey   struct{}
	dropEveryKey struct{}
	reorderKey   struct{}
)

// WithNetwork sets the shared fabric a memory transport registers on and
// delivers through.
func WithNetwork(nw *Network) gossip.Option {
	return func(o *gossip.Options) {
		o.Context = context.WithValue(o.Context, networkKey{}, nw)
	}
}

func networkFrom(ctx context.Context) *Network {
	nw, _ := ctx.Value(networkKey{}).(*Network)
	return nw
}

// WithDropEvery makes the transport drop every nth message it would
// otherwise deliver, modeling reproducible packet loss.
func WithDropEvery(n int) gossip.Option {
	return func(o *gossip.Options) {
		o.Context = context.WithValue(o.Context, dropEveryKey{}, n)
	}
}

func dropEveryFrom(ctx context.Context) int {
	n, _ := ctx.Value(dropEveryKey{}).(int)
	return n
}

// WithReorder makes the transport deliver messages to each destingation
// with adjacent pairs swapped, modeling reproducible out-of-order
// delivery.
func WithReorder() gossip.Option {
	return func(o *gossip.Options) {
		o.Context = context.WithValue(o.Context, reorderKey{}, true)
	}
}

func reorderFrom(ctx context.Context) bool {
	on, _ := ctx.Value(reorderKey{}).(bool)
	return on
}
