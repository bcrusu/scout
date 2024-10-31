package client

import (
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	clusterName       string
	discovery         discovery.Discovery
	dialOptions       []grpc.DialOption
	resolveInterval   time.Duration
	resolveThrottle   time.Duration
	reconnectInterval time.Duration
}

func newOptions() *options {
	return &options{
		resolveInterval:   5 * time.Second,
		resolveThrottle:   time.Second,
		reconnectInterval: time.Second,
	}
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

// WithReconnectInterval configures the reconnect interval.
func WithReconnectInterval(interval time.Duration) Option {
	return func(o *options) {
		o.reconnectInterval = interval
	}
}
