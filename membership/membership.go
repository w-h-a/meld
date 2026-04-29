// Package membership defines the interface for cluster membership
// and failure detection. Implementations track which nodes are
// alive, suspect, or failed.
package membership

import "context"

// State represents a node's membership state.
type State int

const (
	Alive State = iota
	Suspect
	Failed
	Left
)

// Node is a member of the cluster.
type Node struct {
	ID      string
	Address string
	Meta    map[string]string
	State   State
}

// Event represents a membership change.
type Event struct {
	Type EventType
	Node Node
}

type EventType int

const (
	Join EventType = iota
	Leave
	Fail
	Update
)

// Membership is the port interface for cluster membership.
// Implementations handle join/leave protocol and failure detection.
type Membership interface {
	Join(ctx context.Context, existing []string) error
	Leave(ctx context.Context) error
	Members() []Node
	LocalNode() Node
	Watch() (<-chan Event, error)
}
