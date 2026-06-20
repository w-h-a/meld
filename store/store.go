package store

import "context"

type Store interface {
	Save(ctx context.Context, data []byte) error
	Load(ctx context.Context) ([]byte, bool, error)
}
