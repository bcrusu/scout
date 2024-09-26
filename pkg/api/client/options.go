package client

import (
	"github.com/bcrusu/graph/internal/discovery"
	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	clusterName string
	discovery   discovery.Discovery
	dialOptions []grpc.DialOption
}

// WithClusterName sets the cluster name.
func WithClusterName(clusterName string) Option {
	return func(o *options) {
		o.clusterName = clusterName
	}
}

// WithDiscovery sets the connecton target.
func WithDiscovery(discovery discovery.Discovery) Option {
	return func(o *options) {
		o.discovery = discovery
	}
}

// WithDialOptions configures the gRPC connection dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOptions = opts
	}
}
