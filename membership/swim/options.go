package swim

import (
	"context"
	"time"

	"github.com/w-h-a/meld/membership"
)

type (
	probeIntervalKey      struct{}
	probeTimeoutKey       struct{}
	indirectProbeCountKey struct{}
	suspicionMultKey      struct{}
)

// WithProbeInterval sets the period between probe rounds.
func WithProbeInterval(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, probeIntervalKey{}, d)
	}
}

func probeIntervalFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(probeIntervalKey{}).(time.Duration); ok {
		return v
	}
	return time.Second
}

// WithProbeTimeout sets how long to wait for a direct ack before
// falling back to indirect probes.
func WithProbeTimeout(d time.Duration) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, probeTimeoutKey{}, d)
	}
}

func probeTimeoutFrom(ctx context.Context) time.Duration {
	if v, ok := ctx.Value(probeTimeoutKey{}).(time.Duration); ok {
		return v
	}
	return 500 * time.Millisecond
}

// WithIndirectProbeCount sets k, the number of peers asked to ping the
// suspect indirectly.
func WithIndirectProbeCount(k int) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, indirectProbeCountKey{}, k)
	}
}

func indirectProbeCountFrom(ctx context.Context) int {
	if v, ok := ctx.Value(indirectProbeCountKey{}).(int); ok {
		return v
	}
	return 3
}

// WithSuspicionMult sets the multiplier applied to the probe interval
// to derive the suspicion timeout.
func WithSuspicionMult(m int) membership.Option {
	return func(o *membership.Options) {
		o.Context = context.WithValue(o.Context, suspicionMultKey{}, m)
	}
}

func suspicionMultFrom(ctx context.Context) int {
	if v, ok := ctx.Value(suspicionMultKey{}).(int); ok {
		return v
	}
	return 4
}
