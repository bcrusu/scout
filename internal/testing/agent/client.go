package agent

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ ServiceClient = (*Client)(nil)
)

type Client struct {
	ServiceClient
	address string
	conn    *grpc.ClientConn
}

func NewClient(nodeIP string) (*Client, error) {
	c := &Client{
		address: utils.JoinHostPort(nodeIP, Port),
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

	conn, err := grpc.NewClient(c.address, options...)
	if err != nil {
		return errors.Wrapf(err, "connection failed %s", c.address)
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
