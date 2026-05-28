// Package gossip defines the interface for gossip-based state
// propagation. Implementations handle the transport for
// disseminating CRDT state and membership changes.
package gossip

import (
	"context"
	"net"
)

// Packet is a received message with its sender's network address.
type Packet struct {
	From net.Addr
	Data []byte
}

// Gossip is the port interface for state propagation.
type Gossip interface {
	Addr(ctx context.Context) net.Addr
	Listen(ctx context.Context) (<-chan *Packet, error)
	Broadcast(ctx context.Context, msg []byte) error
	SetPeers(ctx context.Context, peers ...string) error
	Stop(ctx context.Context) error
}
