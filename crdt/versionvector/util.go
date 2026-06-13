package versionvector

import "github.com/w-h-a/meld/crdt"

func breakTie(a, b []crdt.Dot) int {
	n := min(len(b), len(a))

	for i := range n {
		if cmp := a[i].Compare(b[i]); cmp != 0 {
			return cmp
		}
	}

	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
