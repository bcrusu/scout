package client

import (
	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	dialOptions []grpc.DialOption
}

// WithDialOptions configures the gRPC connection dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOptions = opts
	}
}
