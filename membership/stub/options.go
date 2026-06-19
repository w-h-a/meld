package stub

import (
	"context"

	"github.com/w-h-a/meld/membership"
)

type membersKey struct{}

// WithMembers sets the fixed cluster teh stub reports from Members.
func WithMembers(members ...membership.Node) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, membersKey{}, members)
	}
}

func membersFrom(ctx context.Context) []membership.Node {
	members, _ := ctx.Value(membersKey{}).([]membership.Node)
	return members
}
