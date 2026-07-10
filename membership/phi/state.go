package phi

import (
	"math"
	"time"
)

// phiState is how the detector currently regards a peer.
type phiState string

const (
	trust   phiState = "trust"
	suspect phiState = "suspect"
)

// nextState decides whether we trust or suspect a peer, given its current
// phi and two thresholds.
//
// If the peer is currently suspected, we change it to trust once phi
// is low enough; else, we keep it at suspected.
// Otherwise, the peer is trusted, and we change it to suspect once phi
// is high enough; else, we keep it at trust.
func nextState(cur phiState, phi, low, high float64) phiState {
	if cur == suspect {
		if phi <= low {
			return trust
		}
		return suspect
	}

	if phi >= high {
		return suspect
	}
	return trust
}

// phiFromWindow returns phi, a suspicion score for one peer. The higher
// the score, the more likely it is that the peer is probably gone. Phi of 1
// marks a wait we would see only about 1 time in 10, phi of 2 about 1 in 100,
// and so on.
func phiFromWindow(
	w *sampleWindow,
	sinceLast time.Duration,
	acceptablePause time.Duration,
	minStdDev time.Duration,
) float64 {
	if w.n == 0 {
		return 0
	}

	beyondAcceptable := max(sinceLast-acceptablePause, 0)

	mu := float64(w.avg())
	sigma := float64(w.stdDev())

	// Guard: A perfectly steady peer has sigma 0, and the calculation of z
	// below would then send phi to infinity. So we need a floor that is not 0.
	floor := float64(minStdDev)
	if sigma < floor {
		sigma = floor
	}

	// Guard: If the caller passed in 0, we just return 0 and halt reasoning.
	if sigma <= 0 {
		return 0
	}

	// z is the number of standard deviations away from the mean our wait was.
	z := (float64(beyondAcceptable) - mu) / sigma

	// pLater is the probability of waiting greater than z standard deviations.
	// math.Erfc(z / math.Sqrt2) gives the probability a gap lands more than
	// z standard deviations from the average on either side. But we only care
	// about one side (waiting longer), so we multiply by 0.5.
	pLater := 0.5 * math.Erfc(z/math.Sqrt2)

	// Guard: pLater can shrink to nothing and the log of 0 is infinity, so we
	// stop it just above 0 to keep phi a real number.
	if pLater < math.SmallestNonzeroFloat64 {
		pLater = math.SmallestNonzeroFloat64
	}

	// pLater is an awkward tiny decimal, so we turn it into a friendly
	// score. -log10 maps 0.1 to 1, 0.01 to 2, and so on, roughly counting
	// leading zeros. That score is phi. So the less probable waiting even
	// longer than we have is, the smaller pLater is and the larger phi is.
	return -math.Log10(pLater)
}

// sampleWindow is a ring of one peer's most recent heartbeat gaps. A gap
// is the time we waited from one heartbeat to the next.
//
// A peer that pings every 100ms might give us gaps of 98ms, 103ms, 99ms,
// and 101ms. Once we know the typical size, an unusually long gap stands
// out. A later gap of 250 sits well outside that spread, and that is our
// signal that the peer might be gone.
type sampleWindow struct {
	capacity int             // how many recent gaps to keep
	samples  []time.Duration // ring buffer of durations (gaps)
	head     int             // where the next gap goes and the oldest gap once full
	n        int             // how many gaps we have so far
	sum      time.Duration   // running total of durations (gaps), for average
}

func newSampleWindow(capacity int) *sampleWindow {
	return &sampleWindow{
		capacity: capacity,
		samples:  make([]time.Duration, capacity),
	}
}

// add records one new gap.
//
// Example: Suppose a full window holds 98, 103, 99, 101 and head is 0.
// Now we want to add a gap of 104. old is 98, so 104 takes that slot.
// We adjust n if necessary, and the sum shifts appropriately.
//
// Example: Suppose a window holds 98, 103, _, _ and head is 2. Now we
// want to add a gap of 99. old is 0 (empty), so 99 takes that slot.
// We adjust n if necessary, and the sum shifts appropriately.
func (w *sampleWindow) add(gap time.Duration) {
	old := w.samples[w.head]
	w.samples[w.head] = gap
	w.head = (w.head + 1) % w.capacity

	if w.n < w.capacity {
		w.n++
	}

	w.sum += gap - old
}

// avg returns the arithmetic mean of the gaps.
func (w *sampleWindow) avg() time.Duration {
	if w.n == 0 {
		return 0
	}
	return w.sum / time.Duration(w.n)
}

// stdDev returns the standard deviation of the gaps.
//
// An empty window has nothing to measure, so it returns 0. Otherwise,
// first, we take the average. Then we add up how far each gap sits
// from that average (squared) and divide by the count. The square
// root of that is the standard deviation.
func (w *sampleWindow) stdDev() time.Duration {
	if w.n == 0 {
		return 0
	}

	mean := float64(w.sum) / float64(w.n)

	var sumSqDev float64
	for i := 0; i < w.n; i++ {
		dev := float64(w.samples[i]) - mean
		sumSqDev += dev * dev
	}

	return time.Duration(math.Sqrt(sumSqDev / float64(w.n)))
}
