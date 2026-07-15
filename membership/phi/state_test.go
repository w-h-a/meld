package phi

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/meld/membership"
)

func TestNextOnHeartbeat_FailedReclaimed(t *testing.T) {
	// arrange
	cur := membership.Failed

	// act
	next, reclaimed := nextOnHeartbeat(cur)

	// assert
	require.Equal(t, membership.Alive, next)
	require.True(t, reclaimed)
}

func TestNextOnHeartbeat_LeftReclaimed(t *testing.T) {
	// arrange
	cur := membership.Left

	// act
	next, reclaimed := nextOnHeartbeat(cur)

	// assert
	require.Equal(t, membership.Alive, next)
	require.True(t, reclaimed)
}

func TestNextOnHeartbeat_AliveUnchanged(t *testing.T) {
	// arrange
	cur := membership.Alive

	// act
	next, reclaimed := nextOnHeartbeat(cur)

	// assert
	require.Equal(t, membership.Alive, next)
	require.False(t, reclaimed)
}

func TestNextOnHeartbeat_SuspectUnchanged(t *testing.T) {
	// arrange
	cur := membership.Suspect

	// act
	next, reclaimed := nextOnHeartbeat(cur)

	// assert
	require.Equal(t, membership.Suspect, next)
	require.False(t, reclaimed)
}

func TestNextOnTick_AliveHoldsBelowHigh(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Alive, 5, low, high, false, false)

	// assert
	require.Equal(t, membership.Alive, next)
	require.False(t, reap)
}

func TestNextOnTick_AliveFlipsToSuspectAtHigh(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Alive, 8, low, high, false, false)

	// assert
	require.Equal(t, membership.Suspect, next)
	require.False(t, reap)
}

func TestNextOnTick_SuspectRecoversToAliveAtLow(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Suspect, 1, low, high, false, false)

	// assert
	require.Equal(t, membership.Alive, next)
	require.False(t, reap)
}

func TestNextOnTick_SuspectHoldsBetweenLowAndHigh(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Suspect, 5, low, high, false, false)

	// assert
	require.Equal(t, membership.Suspect, next)
	require.False(t, reap)
}

func TestNextOnTick_SuspectHoldsAtHighBeforeDwell(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Suspect, 8, low, high, false, false)

	// assert
	require.Equal(t, membership.Suspect, next)
	require.False(t, reap)
}

func TestNextOnTick_SuspectFailsAtHighAfterDwell(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Suspect, 8, low, high, true, false)

	// assert
	require.Equal(t, membership.Failed, next)
	require.False(t, reap)
}

func TestNextOnTick_SuspectHoldsBelowHighAfterDwell(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Suspect, 5, low, high, true, false)

	// assert
	require.Equal(t, membership.Suspect, next)
	require.False(t, reap)
}

func TestNextOnTick_FailedHoldsBeforeReap(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Failed, 0, low, high, false, false)

	// assert
	require.Equal(t, membership.Failed, next)
	require.False(t, reap)
}

func TestNextOnTick_FailedReapedAfterReapDwell(t *testing.T) {
	// arrange
	const low, high = 1.0, 8.0

	// act
	next, reap := nextOnTick(membership.Failed, 0, low, high, false, true)

	// assert
	require.Equal(t, membership.Failed, next)
	require.True(t, reap)
}

func TestPhiFromWindow_MatchesKnownValue(t *testing.T) {
	// arrange
	w := newSampleWindow(2)
	w.add(80 * time.Millisecond)
	w.add(120 * time.Millisecond)

	// avg is 100ms and stdDev is 20ms, so a 200ms wait is
	// (200 - 100) / 20 = z = 5 standard deviations out, which makes
	// phi = ~6.54
	want := -math.Log10(0.5 * math.Erfc(5.0/math.Sqrt2))

	// act
	got := phiFromWindow(w, 200*time.Millisecond, 0, 0)

	// assert
	require.InDelta(t, want, got, 1e-9)
}

func TestPhiFromWindow_EmptyWindowReturnsZero(t *testing.T) {
	// arrange
	w := newSampleWindow(4)

	// act
	got := phiFromWindow(w, 200*time.Millisecond, 0, 100*time.Millisecond)

	// assert
	require.Zero(t, got)
}

func TestPhiFromWindow_SteadyPeerFlooredStaysSane(t *testing.T) {
	// arrange
	w := newSampleWindow(2)
	w.add(100 * time.Millisecond)
	w.add(100 * time.Millisecond)

	// stdDev is 0 here, so the 100ms floor stands in for sigma. A 200ms
	// wait is then (200 - 100) / 100 = 1 standard deviation out, so z
	// is 1 and phi stays small, ~0.8.
	want := -math.Log10(0.5 * math.Erfc(1.0/math.Sqrt2))

	// act
	got := phiFromWindow(w, 200*time.Millisecond, 0, 100*time.Millisecond)

	// assert
	require.InDelta(t, want, got, 1e-9)
}

func TestPhiFromWindow_FarTailStaysFinite(t *testing.T) {
	// arrange
	w := newSampleWindow(2)
	w.add(80 * time.Millisecond)
	w.add(120 * time.Millisecond)

	// act: wait 10x the average
	got := phiFromWindow(w, 1000*time.Millisecond, 0, 0)

	// assert
	require.False(t, math.IsInf(got, 0), "phi must stay finite")
	require.False(t, math.IsNaN(got), "phi must not be NaN")
	require.Greater(t, got, 8.0)
}

func TestPhiFromWindow_ZeroFloorSteadyPeerReturnsZero(t *testing.T) {
	// arrange
	w := newSampleWindow(2)
	w.add(100 * time.Millisecond)
	w.add(100 * time.Millisecond)

	// act: stdDev is 0 and the floor is 0, so there is no spread to judge
	got := phiFromWindow(w, 200*time.Millisecond, 0, 0)

	// assert
	require.Zero(t, got)
}

func TestSampleWindow_AvgAndStdDev(t *testing.T) {
	// arrange
	w := newSampleWindow(4)
	w.add(80 * time.Millisecond)
	w.add(120 * time.Millisecond)

	// act
	avg := w.avg()
	stdDev := w.stdDev()

	// assert
	require.Equal(t, 100*time.Millisecond, avg)
	require.Equal(t, 20*time.Millisecond, stdDev)
}

func TestSampleWindow_EvictsOldestAtCapacity(t *testing.T) {
	// arrange
	w := newSampleWindow(2)
	w.add(100 * time.Millisecond)
	w.add(100 * time.Millisecond)
	w.add(200 * time.Millisecond)

	// act
	avg := w.avg()

	// assert
	require.Equal(t, 150*time.Millisecond, avg)
}
