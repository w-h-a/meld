package store

import "context"

// Option configures a Store.
type Option func(*Options)

// Options hold configuration for a Store.
type Options struct {
	Location string
	Context  context.Context
}

func NewOptions(opts ...Option) Options {
	options := Options{
		Context: context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}

// WithLocation sets where a durable adapter persists its slot.
func WithLocation(loc string) Option {
	return func(o *Options) {
		o.Location = loc
	}
}
