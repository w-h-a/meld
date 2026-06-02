package membership

import (
	"context"

	"github.com/w-h-a/meld/gossip"
)

// Option configures a Membership adapter.
type Option func(*Options)

// Options hold configuration shared by all Membership adapters.
type Options struct {
	Gossip           gossip.Gossip
	NodeID           string
	AdvertiseAddress string
	Meta             map[string]string
	Context          context.Context
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

// WithGossip sets the gossip transport used by the adapter for
// probes, acks, and broadcast dissemination. Required.
func WithGossip(g gossip.Gossip) Option {
	return func(o *Options) {
		o.Gossip = g
	}
}

// WithNodeID sets the local node's identity. Required.
func WithNodeID(id string) Option {
	return func(o *Options) {
		o.NodeID = id
	}
}

// WithAdvertiseAddress sets the address this node tells peers to
// use to reach it. Distinct from the gossip transport's bind
// address: bind is the local interface to listen on (often
// "0.0.0.0:port"); advertise is what we put on the wire so peers
// can send to us.
func WithAdvertiseAddress(addr string) Option {
	return func(o *Options) {
		o.AdvertiseAddress = addr
	}
}

// WithMeta sets the local node's metadata.
func WithMeta(m map[string]string) Option {
	return func(o *Options) {
		o.Meta = m
	}
}
