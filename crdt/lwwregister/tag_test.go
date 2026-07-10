package lwwregister_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/lwwregister"
)

func TestTag_LessIsLexOverCounterThenWriter(t *testing.T) {
	cases := []struct {
		name string
		a, b lwwregister.Tag
		want bool
	}{
		{
			"smaller counter wins regardless of writer",
			lwwregister.Tag{Counter: 1, Writer: "n9"},
			lwwregister.Tag{Counter: 2, Writer: "n1"},
			true,
		},
		{
			"larger counter beats smaller",
			lwwregister.Tag{Counter: 3, Writer: "n1"},
			lwwregister.Tag{Counter: 2, Writer: "n9"},
			false,
		},
		{
			"counter ties, smaller writer is less",
			lwwregister.Tag{Counter: 1, Writer: "n1"},
			lwwregister.Tag{Counter: 1, Writer: "n2"},
			true,
		},
		{
			"counter ties, larger writer is not less",
			lwwregister.Tag{Counter: 1, Writer: "n2"},
			lwwregister.Tag{Counter: 1, Writer: "n1"},
			false,
		},
		{
			"equal tags are not less than each other",
			lwwregister.Tag{Counter: 1, Writer: "n1"},
			lwwregister.Tag{Counter: 1, Writer: "n1"},
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// act + assert
			require.Equal(t, c.want, c.a.Less(c.b))
		})
	}
}
