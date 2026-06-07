package vclock

func breakTie(a, b []counter) int {
	n := min(len(b), len(a))

	for i := range n {
		switch {
		case a[i].id < b[i].id:
			return -1
		case a[i].id > b[i].id:
			return 1
		case a[i].value < b[i].value:
			return -1
		case a[i].value > b[i].value:
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
