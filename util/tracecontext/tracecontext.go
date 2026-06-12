// Package tracecontext moves W3C trace context across a meld message
// boundary. A sender injects the active span context into a string
// carrier that rides along in the message frame. A receiver extracts it
// and starts its handler span as a child of the sender's span. So one
// logical operation that fans out across the network becomes one
// distributed trace instead of a forest of orphaned roots.
//
// The SWIM envelope and every CRDT gossip frame carry the same
// map[string]string carrier field. They share these two functions rather
// than each re-deriving the otel propagator calls.
package tracecontext

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Inject returns a fresh carrier holding the span context active in ctx,
// encoded by the globally registered propagator. The caller stores it on
// the carrier field it gossips:
//
//	e.Carrier = tracecontext.Inject(ctx)
//
// When ctx has no active span the propagator writes nothing and the
// carrier comes back empty, which Extract reads as "no parent."
func Inject(ctx context.Context) map[string]string {
	if ctx == nil {
		return nil
	}

	carrier := map[string]string{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))

	if len(carrier) == 0 {
		return nil
	}

	return carrier
}

// Extract returns a context carrying the remote span context encoded in
// carrier. An empty carrier yields ctx unchanged, so a receiver whose
// sender never injected still starts a valid root span.
func Extract(ctx context.Context, carrier map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	if len(carrier) == 0 {
		return ctx
	}

	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}
