package nodes

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ ServiceClient = (*Client)(nil)
)

type Client struct {
	ServiceClient
	socketPath string
	conn       *grpc.ClientConn
}

func NewClient(socketPath string) (*Client, error) {
	c := &Client{
		socketPath: socketPath,
	}

	if err := c.start(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) start() error {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			interceptors.UnaryValidatorClientInterceptor(),
			interceptors.UnaryErrorsClientInterceptor(),
			interceptors.UnaryLoggerClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			interceptors.StreamValidatorClientInterceptor(),
			interceptors.StreamErrorsClientInterceptor(),
			interceptors.StreamLoggerClientInterceptor(),
		),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceconfig.DefaultServiceConfig().ToJson()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: 3 * time.Second,
		}),
	}

	target := "unix:" + c.socketPath
	conn, err := grpc.NewClient(target, options...)
	if err != nil {
		return errors.Wrapf(err, "connection failed %s", target)
	}

	c.conn = conn
	c.ServiceClient = NewServiceClient(c.conn)
	conn.Connect()
	return nil
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		log.WithError(err).Errorf("Failed to close client connection")
	}
}
