package server

import (
	"context"

	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service = (*GraphService)(nil)
)

// GraphService represents the graph service.
type GraphService struct {
	api.UnimplementedGraphServer
}

// NewGraphService returns a new GraphService instance
func NewGraphService() *GraphService {
	return &GraphService{}
}

func (s *GraphService) RegisterToServer(server *grpc.Server) {
	api.RegisterGraphServer(server, s)
}

// TODO
func (s *GraphService) GetVertex(context.Context, *api.GetVertexRequest) (*api.Vertex, error) {
	return nil, nil
}

func (s *GraphService) SetVertex(context.Context, *api.Vertex) (*api.Status, error) {
	return nil, nil
}

func (s *GraphService) DeleteVertex(context.Context, *api.VertexId) (*api.Status, error) {
	return nil, nil
}

func (s *GraphService) GetEdge(context.Context, *api.EdgeId) (*api.Edge, error) {
	return nil, nil
}

func (s *GraphService) SetEdge(context.Context, *api.EdgeId) (*api.Status, error) {
	return nil, nil
}

func (s *GraphService) DeleteEdge(context.Context, *api.EdgeId) (*api.Status, error) {
	return nil, nil
}
