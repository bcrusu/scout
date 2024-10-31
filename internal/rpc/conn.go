package rpc

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	_ "github.com/bcrusu/scout/internal/rpc/routing" // registers resolvers and balancers
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_     utils.Lifecycle = (*Conn)(nil)
	logRC                 = logging.New("rpc_conn").NoContext()
)

// ConnConfig is the gRPC client configuration.
type ConnConfig struct {
	Target      string
	ClusterName string
	EnableHlc   bool
}

// Conn represents the client connection.
type Conn struct {
	*grpc.ClientConn
	target string
	opts   []grpc.DialOption
}

// NewConn creates a new connection for the provied target and dial options.
func NewConn(config ConnConfig, opts ...grpc.DialOption) *Conn {
	all := DefaultDialOptions(config.ClusterName, config.EnableHlc)
	all = append(all, opts...)

	return &Conn{
		target: config.Target,
		opts:   all,
	}
}

// NewAdminConn creates a new admin cli tool connection.
func NewAdminConn(target string, opts ...grpc.DialOption) *Conn {
	all := AdminDialOptions()
	all = append(all, opts...)

	return &Conn{
		target: target,
		opts:   all,
	}
}

// DefaultDialOptions returns the default Conn dial options.
func DefaultDialOptions(clusterName string, enableHlc bool) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO
		grpc.WithChainUnaryInterceptor(
			interceptors.UnaryAuthClientInterceptor(clusterName),
			interceptors.UnaryHlcClientInterceptor(enableHlc),
			interceptors.UnaryMetadataClientInterceptor(),
			interceptors.UnaryValidatorClientInterceptor(),
			interceptors.UnaryErrorsClientInterceptor(),
			interceptors.UnaryLoggerClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			interceptors.StreamAuthClientInterceptor(clusterName),
			interceptors.StreamHlcClientInterceptor(enableHlc),
			interceptors.StreamMetadataClientInterceptor(),
			interceptors.StreamValidatorClientInterceptor(),
			interceptors.StreamErrorsClientInterceptor(),
			interceptors.StreamLoggerClientInterceptor(),
		),
		grpc.WithDefaultServiceConfig(serviceconfig.DefaultServiceConfig().ToJson()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: 3 * time.Second,
		}),
	}
}

// AdminDialOptions returns the dial options for admin calls.
// Used by cmd/admin cli tool.
func AdminDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO
		grpc.WithChainUnaryInterceptor(
			interceptors.UnaryMetadataClientInterceptor(),
			interceptors.UnaryErrorsClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			interceptors.StreamMetadataClientInterceptor(),
			interceptors.StreamErrorsClientInterceptor(),
		),
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

func (c *Conn) Stop() {
	if c.ClientConn == nil {
		return
	}

	if err := c.ClientConn.Close(); err != nil {
		logRC.WithError(err).Errorf("Failed to close client connection")
	}
}
