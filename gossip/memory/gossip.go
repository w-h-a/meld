package memory

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/w-h-a/meld/gossip"
)

// memoryGossip is an in-memory gossip adapter.
type memoryGossip struct {
	options   gossip.Options
	net       *Network
	addr      addr
	inbox     chan *gossip.Packet
	listening atomic.Bool
	once      sync.Once
}

func New(opts ...gossip.Option) (gossip.Gossip, error) {
	options := gossip.NewOptions(opts...)

	nw := networkFrom(options.Context)
	if nw == nil {
		return nil, errors.New("memory: a network is required")
	}

	inbox := make(chan *gossip.Packet, 64)
	nw.register(options.BindAddress, inbox)

	g := &memoryGossip{
		options: options,
		net:     nw,
		addr:    addr(options.BindAddress),
		inbox:   inbox,
	}

	return g, nil
}

// Addr returns this transport's address on the network.
func (g *memoryGossip) Addr(_ context.Context) net.Addr {
	return g.addr
}

// Listen returns the channel of incoming messages. It may be called at
// most once.
func (g *memoryGossip) Listen(_ context.Context) (<-chan *gossip.Packet, error) {
	if !g.listening.CompareAndSwap(false, true) {
		return nil, errors.New("memory: already listening")
	}

	return g.inbox, nil
}

// SendTo delivers msg to a single peer's inbox on the network.
func (g *memoryGossip) SendTo(ctx context.Context, addr net.Addr, msg []byte) error {
	inbox, ok := g.net.lookup(addr.String())
	if !ok {
		err := errors.New("memory: unknown address " + addr.String())
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case inbox <- &gossip.Packet{From: g.addr, Data: msg}:
		return nil
	}
}

// Stop removes the transport from the network. It does not close the
// inbox. The consumer stops by cancelling its receive loop. Safe to
// call repeatedly.
func (g *memoryGossip) Stop(ctx context.Context) error {
	g.once.Do(func() {
		g.net.deregister(g.addr.String())
	})

	return nil
}

// Resolve turns an address string into a net.Addr on the network.
func (g *memoryGossip) Resolve(s string) (net.Addr, error) {
	return addr(s), nil
}

// Broadcast delivers msg to every peer's inbox on the network.
func (g *memoryGossip) Broadcast(ctx context.Context, peers []net.Addr, msg []byte) error {
	var lastErr error
	sent := 0

	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return err
		}

		inbox, ok := g.net.lookup(peer.String())
		if !ok {
			lastErr = errors.New("memory: unknown address " + peer.String())
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case inbox <- &gossip.Packet{From: g.addr, Data: msg}:
			sent++
		}
	}

	if sent == 0 && lastErr != nil {
		return lastErr
	}

	return nil
}
