package client

import (
	"time"

	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	clusterName       string
	address           string
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

// WithAddress sets the connecton address.
func WithAddress(address string) Option {
	return func(o *options) {
		o.address = address
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
