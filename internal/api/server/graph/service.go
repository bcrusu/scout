package graph

import (
	"context"

	"github.com/bcrusu/scout/internal/api/server/txn"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/pkg/graph"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service         = (*Service)(nil)
	_ graph.ServiceServer = (*Service)(nil)
)

// Service represents the graph service.
type Service struct {
	graph.UnsafeServiceServer
	processor *txn.Processor
}

// NewService returns a new Service instance
func NewService(processor *txn.Processor) *Service {
	return &Service{
		processor: processor,
	}
}

func (s *Service) RegisterToServer(server *grpc.Server) {
	graph.RegisterServiceServer(server, s)
}

// TODO
func (s *Service) GetVertex(ctx context.Context, req *graph.GetVertexRequest) (*graph.Vertex, error) {
	return nil, nil
}

func (s *Service) CreateVertex(ctx context.Context, vertex *graph.Vertex) (*graph.Status, error) {
	return nil, nil
}

func (s *Service) UpdateVertex(ctx context.Context, vertex *graph.Vertex) (*graph.Status, error) {
	return nil, nil
}

func (s *Service) DeleteVertex(ctx context.Context, id *graph.VertexId) (*graph.Status, error) {
	return nil, nil
}

func (s *Service) GetEdge(ctx context.Context, req *graph.GetEdgeRequest) (*graph.Edge, error) {
	return nil, nil
}

func (s *Service) CreateEdge(ctx context.Context, edge *graph.Edge) (*graph.Status, error) {
	return nil, nil
}

func (s *Service) UpdateEdge(ctx context.Context, edge *graph.Edge) (*graph.Status, error) {
	return nil, nil
}

func (s *Service) DeleteEdge(ctx context.Context, id *graph.EdgeId) (*graph.Status, error) {
	return nil, nil
}
