package memory

import (
	"sync"

	"github.com/w-h-a/meld/gossip"
)

// addr is an in-memory network address
type addr string

func (a addr) Network() string { return "memory" }
func (a addr) String() string  { return string(a) }

// link is the per-destination delivery state the chaos knobs need.
type link struct {
	count int
	held  *gossip.Packet
}

// Network is the shared fabric memory transports register on to
// deliver to each other's inboxes.
type Network struct {
	mtx     sync.RWMutex
	inboxes map[string]chan *gossip.Packet
}

func NewNetwork() *Network {
	return &Network{inboxes: map[string]chan *gossip.Packet{}}
}

func (nw *Network) register(address string, inbox chan *gossip.Packet) {
	nw.mtx.Lock()
	defer nw.mtx.Unlock()

	nw.inboxes[address] = inbox
}

func (nw *Network) lookup(address string) (chan *gossip.Packet, bool) {
	nw.mtx.RLock()
	defer nw.mtx.RUnlock()

	in, ok := nw.inboxes[address]

	return in, ok
}

func (nw *Network) deregister(address string) {
	nw.mtx.Lock()
	defer nw.mtx.Unlock()

	delete(nw.inboxes, address)
}
