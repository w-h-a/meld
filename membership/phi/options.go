package phi

import (
	"context"
	"time"

	"github.com/w-h-a/meld/membership"
)

type (
	heartbeatIntervalKey        struct{}
	windowSizeKey               struct{}
	phiLowThresholdKey          struct{}
	phiHighThresholdKey         struct{}
	minStdDevKey                struct{}
	acceptableHeartbeatPauseKey struct{}
	suspectDwellKey             struct{}
	reapDwellKey                struct{}
)

// WithHeartbeatInterval sets how often this node emits a heartbeat.
func WithHeartbeatInterval(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, heartbeatIntervalKey{}, d)
	}
}

func heartbeatIntervalFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(heartbeatIntervalKey{}).(time.Duration); ok {
		return v
	}
	return time.Second
}

// WithWindowSize sets how many recent heartbeat gaps the detector keeps per
// peer when estimating phi.
func WithWindowSize(n int) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, windowSizeKey{}, n)
	}
}

func windowSizeFrom(ctx context.Context) int {
	if v, ok := ctx.Value(windowSizeKey{}).(int); ok {
		return v
	}
	return 1000
}

// WithPhiLowThreshold sets the phi value at or below which a peer is trusted.
func WithPhiLowThreshold(f float64) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, phiLowThresholdKey{}, f)
	}
}

func phiLowThresholdFrom(ctx context.Context) float64 {
	if v, ok := ctx.Value(phiLowThresholdKey{}).(float64); ok {
		return v
	}
	return 1.0
}

// WithPhiHighThreshold sets the phi value at or above which a peer is suspected.
func WithPhiHighThreshold(f float64) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, phiHighThresholdKey{}, f)
	}
}

func phiHighThresholdFrom(ctx context.Context) float64 {
	if v, ok := ctx.Value(phiHighThresholdKey{}).(float64); ok {
		return v
	}
	return 8.0
}

// WithMinStdDev sets the floor on the heartbeat gap standard deviation, so a
// very steady peer does not make phi explode over a tiny jitter.
func WithMinStdDev(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, minStdDevKey{}, d)
	}
}

func minStdDevFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(minStdDevKey{}).(time.Duration); ok {
		return v
	}
	return 100 * time.Millisecond
}

// WithAcceptableHeartbeatPause sets a grace budget subtracted from the wait
// before computing phi, so that brief hiccups are forgiven.
func WithAcceptableHeartbeatPause(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, acceptableHeartbeatPauseKey{}, d)
	}
}

func acceptableHeartbeatPauseFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(acceptableHeartbeatPauseKey{}).(time.Duration); ok {
		return v
	}
	return 0
}

// WithSuspectDwell sets how long phi must stay at or above the threshold
// before a suspected peer is marked Failed.
func WithSuspectDwell(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, suspectDwellKey{}, d)
	}
}

func suspectDwellFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(suspectDwellKey{}).(time.Duration); ok {
		return v
	}
	return 0
}

// WithReapDwell sets how long a peer stays Failed before the checker forgets
// it.
func WithReapDwell(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, reapDwellKey{}, d)
	}
}

func reapDwellFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(reapDwellKey{}).(time.Duration); ok {
		return v
	}
	return 30 * time.Second
}
