package swim

import "github.com/w-h-a/meld/membership"

// nodeState is SWIM's per-node view.
// Incarnation is a per-node version number that lets a node
// correct false rumors about itself.
// Example. A is healthy, but a network blip prevents B from
// reaching it. B starts gossiping "A is Suspect, incarnation
// 5", and the rumor spreads. When A learns it has been marked
// Suspect, it bumps its own number to 6 and broadcasts "A is
// Alive, incarnation 6".
type nodeState struct {
	ID          string
	Address     string
	Meta        map[string]string
	State       membership.State
	Incarnation uint64
}

// apply merges an incoming view of a node into the local view, checking the
// cases below in order. It returns the merged state and whether anything
// changed.
//
//  1. Suppose we hold the node as Left. No matter the incoming state, if the
//     incoming incarnation is higher than the one we hold, we reclaim it; else,
//     we ignore it.
//  2. Suppose the incoming view says the node is Left. Leave is authoritative
//     so we take it even at a lower incarnation, but record the nodeState with
//     the higher incarnation.
//  3. Higher incarnation wins.
//  4. At equal incarnation, Failed > Suspect > Alive.
func apply(local, incoming nodeState) (nodeState, bool) {
	// 1.
	if local.State == membership.Left {
		if incoming.Incarnation > local.Incarnation {
			return incoming, true
		}
		return local, false
	}

	// 2.
	if incoming.State == membership.Left {
		if local.Incarnation > incoming.Incarnation {
			next := local
			next.State = membership.Left
			return next, true
		}
		return incoming, true
	}

	// 3.
	if incoming.Incarnation < local.Incarnation {
		return local, false
	}
	if incoming.Incarnation > local.Incarnation {
		return incoming, true
	}

	// 4.
	if precedence(incoming.State) <= precedence(local.State) {
		return local, false
	}

	return incoming, true
}

func precedence(s membership.State) int {
	switch s {
	case membership.Alive:
		return 0
	case membership.Suspect:
		return 1
	case membership.Failed:
		return 2
	case membership.Left:
		return 3
	}
	return -1
}
