package udp

import (
	"context"

	"github.com/w-h-a/meld/gossip"
)

type mtuKey struct{}

// WithMTU sets the maximum UDP datagram payload size in bytes.
// Messages at or above this size are rejected by Broadcast
// with an error.
func WithMTU(mtu int) gossip.Option {
	return func(o *gossip.Options) {
		o.Context = context.WithValue(o.Context, mtuKey{}, mtu)
	}
}

func mtuFrom(ctx context.Context) int {
	if v, ok := ctx.Value(mtuKey{}).(int); ok {
		return v
	}
	return 1400
}
