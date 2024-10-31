package client

import (
	"time"

	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	clusterName       string
	dialOptions       []grpc.DialOption
	resolveThrottle   time.Duration
	reconnectInterval time.Duration
}

func newOptions() *options {
	return &options{
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
