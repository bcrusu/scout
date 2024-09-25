package client

import (
	"context"

	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ api.KeyValueClient = (*keyValueClient)(nil)
)

type keyValueClient struct {
	client api.KeyValueClient
}

func (c *keyValueClient) Set(ctx context.Context, req *api.SetRequest, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.Set(ctx, req, opts...)
}

func (c *keyValueClient) Get(ctx context.Context, req *api.GetRequest, opts ...grpc.CallOption) (*api.GetResponse, error) {
	return c.client.Get(ctx, req, opts...)
}

func (c *keyValueClient) Delete(ctx context.Context, req *api.DeleteRequest, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.Delete(ctx, req, opts...)
}
