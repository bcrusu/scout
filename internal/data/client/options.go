package client

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

type Option func(*options)

type options struct {
	dataServers DataServers
	dialOptions []grpc.DialOption
}

type DataServers interface {
	SubscribeDataServers() utils.Subscriber[*control.DataServers]
}

// WithDataServers sets the data servers source.
func WithDataServers(dataServers DataServers) Option {
	return func(o *options) {
		o.dataServers = dataServers
	}
}

// WithDialOptions configures the gRPC connection dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOptions = opts
	}
}
