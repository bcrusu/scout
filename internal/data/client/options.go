package client

import (
	"github.com/bcrusu/graph/internal/discovery"
	"google.golang.org/grpc"
)

// WithTarget sets the connecton target.
func WithTarget(target discovery.Target) Option {
	return func(o *options) {
		o.target = target
	}
}

// WithDialOptions configures the gRPC connection dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOptions = opts
	}
}
