package antientropy

import "github.com/w-h-a/meld/membership"

// PeerAddress resolves a member's anti-entropy transport address.
type PeerAddress func(membership.Node) (addr string, ok bool)

// defaultPeerAddress dials a member's own Address.
func defaultPeerAddress(n membership.Node) (string, bool) {
	return n.Address, n.Address != ""
}
