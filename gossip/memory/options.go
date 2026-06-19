package memory

import (
	"context"

	"github.com/w-h-a/meld/gossip"
)

type networkKey struct{}

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
