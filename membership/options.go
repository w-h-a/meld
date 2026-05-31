package membership

import (
	"context"
)

// Option configures a Membership adapter.
type Option func(*Options)

// Options hold configuration shared by all Membership adapters.
type Options struct {
	NodeID      string
	BindAddress string
	Meta        map[string]string
	Context     context.Context
}

func NewOptions(opts ...Option) Options {
	options := Options{
		Meta:    map[string]string{},
		Context: context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}

// WithNodeID sets the local node's identity. Required.
func WithNodeID(id string) Option {
	return func(o *Options) {
		o.NodeID = id
	}
}

// WithBindAddress sets the address advertised to peers. When
// empty, the adapter will fall back to its transport's bound address.
func WithBindAddress(addr string) Option {
	return func(o *Options) {
		o.BindAddress = addr
	}
}

// WithMeta sets the local node's metadata.
func WithMeta(m map[string]string) Option {
	return func(o *Options) {
		o.Meta = m
	}
}
