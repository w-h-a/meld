// Package gossip defines the interface for gossip-based state
// propagation. Implementations handle the transport (UDP/TCP)
// for disseminating CRDT state and membership changes.
package gossip

import "context"

// Gossip is the port interface for state propagation.
type Gossip interface {
	Broadcast(ctx context.Context, msg []byte) error
	Listen(ctx context.Context) (<-chan []byte, error)
	Stop(ctx context.Context) error
}
