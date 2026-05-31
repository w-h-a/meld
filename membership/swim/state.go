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

// apply merges an incoming view of a node into the local view.
// Rules, in priority order:
//  1. Leaving the cluster is a permanent goodbye.
//     A node enters this Left state only by calling Leave on itself.
//     That call is authoritative.
//     - If we have already recorded a node as having left, that view
//     is permanent. Any later message about that node is ignored.
//     - If an incoming message says a node has left, we accept
//     that even at a lower incarnation than we currently have. We
//     record the higher of the two incarnations.
//  2. Higher incarnation wins.
//  3. Failed > Suspect > Alive.
//
// Returns the merged state and whether anything observable has changed.
func apply(local, incoming nodeState) (nodeState, bool) {
	if local.State == membership.Left {
		return local, false
	}

	if incoming.State == membership.Left {
		if local.Incarnation > incoming.Incarnation {
			next := local
			next.State = membership.Left
			return next, true
		}
		return incoming, true
	}

	if incoming.Incarnation < local.Incarnation {
		return local, false
	}

	if incoming.Incarnation > local.Incarnation {
		return incoming, true
	}

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
