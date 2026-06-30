package memory

import (
	"context"
	"sync"

	"github.com/w-h-a/meld/store"
)

// memoryStore is an in-memory, single-slot store.
type memoryStore struct {
	options store.Options
	mtx     sync.RWMutex
	data    []byte
	saved   bool
}

func New(opts ...store.Option) (store.Store, error) {
	options := store.NewOptions(opts...)

	s := &memoryStore{
		options: options,
		mtx:     sync.RWMutex{},
	}

	return s, nil
}

// Save persists data as the node's current state, overwriting any prior
// data.
func (s *memoryStore) Save(ctx context.Context, data []byte) error {
	buf := make([]byte, len(data))
	copy(buf, data)

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.data = buf
	s.saved = true

	return nil
}

// Load returns the node's most recently saved data, reporting false until
// the first Save so a fresh node can tell no-prior-state from a failure.
func (s *memoryStore) Load(ctx context.Context) ([]byte, bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if !s.saved {
		return nil, false, nil
	}

	buf := make([]byte, len(s.data))
	copy(buf, s.data)

	return buf, true, nil
}
