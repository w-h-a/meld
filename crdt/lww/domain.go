package lww

// Tag is the totally ordered timestamp attached to the current
// value of a Register.
type Tag struct {
	Counter uint64
	Writer  string
}

// Less reports whether t precedes other in Tag's total order.
func (t Tag) Less(other Tag) bool {
	if t.Counter != other.Counter {
		return t.Counter < other.Counter
	}

	return t.Writer < other.Writer
}
