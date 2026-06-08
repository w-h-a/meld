package lww_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/crdt/lww"
)

func TestTag_LessIsLexOverCounterThenWriter(t *testing.T) {
	cases := []struct {
		name string
		a, b lww.Tag
		want bool
	}{
		{
			"smaller counter wins regardless of writer",
			lww.Tag{Counter: 1, Writer: "n9"},
			lww.Tag{Counter: 2, Writer: "n1"},
			true,
		},
		{
			"larger counter beats smaller",
			lww.Tag{Counter: 3, Writer: "n1"},
			lww.Tag{Counter: 2, Writer: "n9"},
			false,
		},
		{
			"counter ties, smaller writer is less",
			lww.Tag{Counter: 1, Writer: "n1"},
			lww.Tag{Counter: 1, Writer: "n2"},
			true,
		},
		{
			"counter ties, larger writer is not less",
			lww.Tag{Counter: 1, Writer: "n2"},
			lww.Tag{Counter: 1, Writer: "n1"},
			false,
		},
		{
			"equal tags are not less than each other",
			lww.Tag{Counter: 1, Writer: "n1"},
			lww.Tag{Counter: 1, Writer: "n1"},
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
