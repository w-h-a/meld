package stub

import (
	"context"
	"errors"

	"github.com/w-h-a/meld/membership"
)

// stubMembership is a stub membership source.
type stubMembership struct {
	options membership.Options
	local   membership.Node
	members []membership.Node
}

func New(opts ...membership.Option) (membership.Membership, error) {
	options := membership.NewOptions(opts...)

	if options.NodeID == "" {
		return nil, errors.New("stub: a node id is required")
	}

	local := membership.Node{
		ID:      options.NodeID,
		Address: options.AdvertiseAddress,
		Meta:    options.Meta,
		State:   membership.Alive,
	}

	m := &stubMembership{
		options: options,
		local:   local,
		members: membersFrom(options.Context),
	}

	return m, nil
}

// Join is no-op. The stub's cluster is fixed at construction.
func (m *stubMembership) Join(ctx context.Context, existing []string) error {
	return nil
}

// Leave is no-op. The stub's cluster is fixed at construction.
func (m *stubMembership) Leave(ctx context.Context) error {
	return nil
}

// Members returns the fixed cluster.
func (m *stubMembership) Members() []membership.Node {
	return m.members
}

// LocalNode returns this node's identity.
func (m *stubMembership) LocalNode() membership.Node {
	return m.local
}

// Watch returns a closed channel: the stub's cluster never changes, so
// there are no events to emit.
func (m *stubMembership) Watch() (<-chan membership.Event, error) {
	ch := make(chan membership.Event)
	close(ch)
	return ch, nil
}
