package server

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/graph"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service            = (*GraphService)(nil)
	_ api.GraphServiceServer = (*GraphService)(nil)
)

// GraphService represents the graph service.
type GraphService struct {
	api.UnsafeGraphServiceServer
	store *graph.Store
}

// NewGraphService returns a new GraphService instance
func NewGraphService(store *graph.Store) *GraphService {
	return &GraphService{
		store: store,
	}
}

func (s *GraphService) RegisterToServer(server *grpc.Server) {
	api.RegisterGraphServiceServer(server, s)
}

func (s *GraphService) GetVertex(ctx context.Context, req *api.GetVertexRequest) (*api.Vertex, error) {
	return s.store.GetVertex(ctx, req)
}

func (s *GraphService) UpdateVertex(ctx context.Context, vertex *api.Vertex) (*api.Status, error) {
	return s.store.UpdateVertex(ctx, vertex)
}

func (s *GraphService) DeleteVertex(ctx context.Context, id *api.VertexId) (*api.Status, error) {
	return s.store.DeleteVertex(ctx, id)
}

func (s *GraphService) GetEdge(ctx context.Context, req *api.GetEdgeRequest) (*api.Edge, error) {
	return s.store.GetEdge(ctx, req)
}

func (s *GraphService) UpdateEdge(ctx context.Context, edge *api.Edge) (*api.Status, error) {
	return s.store.UpdateEdge(ctx, edge)
}

func (s *GraphService) DeleteEdge(ctx context.Context, id *api.EdgeId) (*api.Status, error) {
	return s.store.DeleteEdge(ctx, id)
}
