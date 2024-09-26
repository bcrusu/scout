package client

import (
	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	clusterName string
	dialOptions []grpc.DialOption
}

// WithClusterName sets the cluster name.
func WithClusterName(clusterName string) Option {
	return func(o *options) {
		o.clusterName = clusterName
	}
}

// WithDialOptions configures the gRPC connection dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOptions = opts
	}
}
