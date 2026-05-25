// Package gossip defines the interface for gossip-based state
// propagation. Implementations handle the transport for
// disseminating CRDT state and membership changes.
package gossip

import (
	"context"
	"net"
)

// Gossip is the port interface for state propagation.
type Gossip interface {
	Addr(ctx context.Context) net.Addr
	Listen(ctx context.Context) (<-chan []byte, error)
	Broadcast(ctx context.Context, msg []byte) error
	Stop(ctx context.Context) error
}
