package rpc

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/interceptors"
	_ "github.com/bcrusu/graph/internal/rpc/routing" // registers resolvers and balancers
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_     utils.Lifecycle = (*Conn)(nil)
	logRC                 = logging.WithComponent("rpc_conn")
)

// Conn represents the client connection.
type Conn struct {
	*grpc.ClientConn
	target string
	opts   []grpc.DialOption
}

// NewConn creates a new connection for the provied target and dial options.
func NewConn(target string, opts ...grpc.DialOption) *Conn {
	all := DefaultDialOptions()
	all = append(all, opts...)

	return &Conn{
		target: target,
		opts:   all,
	}
}

// DefaultDialOptions returns the default Conn dial options.
func DefaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO
		grpc.WithChainUnaryInterceptor(
			interceptors.UnaryMetadataClientInterceptor(),
			interceptors.UnaryLoggerClientInterceptor(),
			interceptors.UnaryErrorsClientInterceptor()),
		grpc.WithChainStreamInterceptor(
			interceptors.StreamMetadataClientInterceptor(),
			interceptors.StreamLoggerClientInterceptor(),
			interceptors.StreamErrorsClientInterceptor()),
		grpc.WithDefaultServiceConfig(serviceconfig.DefaultServiceConfig().ToJson()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: 3 * time.Second,
		}),
	}
}

func (c *Conn) Start(ctx context.Context) error {
	conn, err := grpc.NewClient(c.target, c.opts...)
	if err != nil {
		return errors.Wrap(err, "failed to create client connection")
	}

	c.ClientConn = conn
	conn.Connect()
	return nil
}

func (c *Conn) Stop(ctx context.Context) {
	if c.ClientConn == nil {
		return
	}

	if err := c.ClientConn.Close(); err != nil {
		logRC.WithError(err).Errorf(ctx, "Failed to close client connection")
	}
}
