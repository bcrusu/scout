package graph

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/txn"
	"github.com/bcrusu/graph/pkg/api"
)

type Store struct {
	processor *txn.Processor
}

func NewStore(processor *txn.Processor) *Store {
	return &Store{
		processor: processor,
	}
}

// TODO
func (s *Store) GetVertex(ctx context.Context, req *api.GetVertexRequest) (*api.Vertex, error) {
	return nil, nil
}

func (s *Store) UpdateVertex(ctx context.Context, vertex *api.Vertex) (*api.Status, error) {
	return nil, nil
}

func (s *Store) DeleteVertex(ctx context.Context, id *api.VertexId) (*api.Status, error) {
	return nil, nil
}

func (s *Store) GetEdge(ctx context.Context, req *api.GetEdgeRequest) (*api.Edge, error) {
	return nil, nil
}

func (s *Store) UpdateEdge(ctx context.Context, edge *api.Edge) (*api.Status, error) {
	return nil, nil
}

func (s *Store) DeleteEdge(ctx context.Context, id *api.EdgeId) (*api.Status, error) {
	return nil, nil
}
