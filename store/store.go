// Package store defines the persistence port for a node's durable state.
package store

import "context"

// Store persists state for a node and reloads it.
type Store interface {
	Save(ctx context.Context, data []byte) error
	Load(ctx context.Context) ([]byte, bool, error)
	Close(ctx context.Context) error
}
