package gossip

import "context"

// Option configures a Gossip transport.
type Option func(*Options)

// Options hold configuration for a Gossip transport.
type Options struct {
	BindAddress string
	Peers       []string
	Fanout      int
	Context     context.Context
}

func NewOptions(opts ...Option) Options {
	options := Options{
		BindAddress: ":0",
		Peers:       []string{},
		Fanout:      3,
		Context:     context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}

// WithBindAddress sets the address to bind for transport
// listeners.
func WithBindAddress(addr string) Option {
	return func(o *Options) {
		o.BindAddress = addr
	}
}

// WithPeers appends known peer addresses.
func WithPeers(peers ...string) Option {
	return func(o *Options) {
		o.Peers = append(o.Peers, peers...)
	}
}

// WithFanout sets the number of randomly selected peers per
// Broadcast call.
func WithFanout(n int) Option {
	return func(o *Options) {
		if n < 1 {
			n = 1
		}
		o.Fanout = n
	}
}
