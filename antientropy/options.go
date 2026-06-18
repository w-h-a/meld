package antientropy

import (
	"context"
	"time"

	"github.com/w-h-a/meld/crdt"
	"github.com/w-h-a/meld/gossip"
	"github.com/w-h-a/meld/membership"
)

// Option configures a Replicator
type Option[T crdt.Mergeable[T]] func(*Options[T])

// Options hold the configuration every Replicator shares.
type Options[T crdt.Mergeable[T]] struct {
	Initial     T
	Encoder     Encoder[T]
	Decoder     Decoder[T]
	PeerAddress PeerAddress
	OnReceive   OnReceive[T]
	OnSend      OnSend[T]
	Transport   gossip.Gossip
	Membership  membership.Membership
	Interval    time.Duration
	Context     context.Context
}

func NewOptions[T crdt.Mergeable[T]](opts ...Option[T]) Options[T] {
	options := Options[T]{
		PeerAddress: defaultPeerAddress,
		Interval:    time.Second,
		Context:     context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}

// WithInitial set sthe starting CRDT state.
func WithInitial[T crdt.Mergeable[T]](initial T) Option[T] {
	return func(o *Options[T]) {
		o.Initial = initial
	}
}

// WithCodec sets how the node serializes a state for the wire and parses
// it back.
func WithCodec[T crdt.Mergeable[T]](encoder Encoder[T], decoder Decoder[T]) Option[T] {
	return func(o *Options[T]) {
		o.Encoder = encoder
		o.Decoder = decoder
	}
}

// WithPeerAddress sets how a member's transport address is resolved.
func WithPeerAddress[T crdt.Mergeable[T]](resolve PeerAddress) Option[T] {
	return func(o *Options[T]) {
		o.PeerAddress = resolve
	}
}

// WithOnReceive sets a hook the node calls after each merge.
func WithOnReceive[T crdt.Mergeable[T]](hook OnReceive[T]) Option[T] {
	return func(o *Options[T]) {
		o.OnReceive = hook
	}
}

// WithOnSend sets a hook the node calls after each gossip round.
func WithOnSend[T crdt.Mergeable[T]](hook OnSend[T]) Option[T] {
	return func(o *Options[T]) {
		o.OnSend = hook
	}
}

// WithTransport sets the gossip transport the node sends and receives on.
func WithTransport[T crdt.Mergeable[T]](g gossip.Gossip) Option[T] {
	return func(o *Options[T]) {
		o.Transport = g
	}
}

// WithMembership sets the source of the peer list.
func WithMembership[T crdt.Mergeable[T]](m membership.Membership) Option[T] {
	return func(o *Options[T]) {
		o.Membership = m
	}
}

// WithInterval sets how often the node gossips.
func WithInterval[T crdt.Mergeable[T]](d time.Duration) Option[T] {
	return func(o *Options[T]) {
		o.Interval = d
	}
}
