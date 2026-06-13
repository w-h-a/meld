package versionvector

import "github.com/w-h-a/meld/crdt"

func breakTie(a, b []crdt.Dot) int {
	n := min(len(b), len(a))

	for i := range n {
		switch {
		case a[i].Node < b[i].Node:
			return -1
		case a[i].Node > b[i].Node:
			return 1
		case a[i].Counter < b[i].Counter:
			return -1
		case a[i].Counter > b[i].Counter:
			return 1
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
