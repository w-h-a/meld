package gossip

import "context"

// Option configures a Gossip transport.
type Option func(*Options)

// Options hold configuration for a Gossip transport.
type Options struct {
	BindAddress string
	Peers       []string
	Context     context.Context
}

func NewOptions(opts ...Option) Options {
	options := Options{
		BindAddress: ":0",
		Peers:       []string{},
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
