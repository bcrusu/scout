package client

import (
	"context"

	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ api.GraphClient = (*graphClient)(nil)
)

type graphClient struct {
	client api.GraphClient
}

func (c *graphClient) GetVertex(ctx context.Context, req *api.GetVertexRequest, opts ...grpc.CallOption) (*api.Vertex, error) {
	return c.client.GetVertex(ctx, req, opts...)
}

func (c *graphClient) SetVertex(ctx context.Context, req *api.Vertex, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.SetVertex(ctx, req, opts...)
}

func (c *graphClient) DeleteVertex(ctx context.Context, req *api.VertexId, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.DeleteVertex(ctx, req, opts...)
}

func (c *graphClient) GetEdge(ctx context.Context, req *api.EdgeId, opts ...grpc.CallOption) (*api.Edge, error) {
	return c.client.GetEdge(ctx, req, opts...)
}

func (c *graphClient) SetEdge(ctx context.Context, req *api.EdgeId, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.SetEdge(ctx, req, opts...)
}

func (c *graphClient) DeleteEdge(ctx context.Context, req *api.EdgeId, opts ...grpc.CallOption) (*api.Status, error) {
	return c.client.DeleteEdge(ctx, req, opts...)
}
