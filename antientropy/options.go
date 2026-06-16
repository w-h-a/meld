package antientropy

import (
	"context"
	"time"

	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/membership"
)

// Option configures a Replicator
type Option func(*Options)

// Options hold the configuration every Replicator shares.
type Options struct {
	Transport  gossip.Gossip
	Membership membership.Membership
	Interval   time.Duration
	Context    context.Context
}

func NewOptions(opts ...Option) Options {
	options := Options{
		Interval: time.Second,
		Context:  context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}

// WithTransport sets the gossip transport the node sends and receives on.
func WithTransport(g gossip.Gossip) Option {
	return func(o *Options) {
		o.Transport = g
	}
}

// WithMembership sets the source of the peer list.
func WithMembership(m membership.Membership) Option {
	return func(o *Options) {
		o.Membership = m
	}
}

// WithInterval sets how often the node gossips.
func WithInterval(d time.Duration) Option {
	return func(o *Options) {
		o.Interval = d
	}
}
