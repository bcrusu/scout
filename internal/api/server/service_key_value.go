package server

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/keyvalue"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service               = (*KeyValueService)(nil)
	_ api.KeyValueServiceServer = (*KeyValueService)(nil)
)

// KeyValueService represents the key-value service.
type KeyValueService struct {
	api.UnsafeKeyValueServiceServer
	store keyvalue.Store
}

// NewKeyValueService returns a new KeyValueService instance
func NewKeyValueService(store keyvalue.Store) *KeyValueService {
	return &KeyValueService{
		store: store,
	}
}

func (s *KeyValueService) RegisterToServer(server *grpc.Server) {
	api.RegisterKeyValueServiceServer(server, s)
}

func (s *KeyValueService) Get(ctx context.Context, req *api.KeyAt) (*api.ValueAt, error) {
	return s.store.Get(ctx, req)
}

func (s *KeyValueService) Set(ctx context.Context, req *api.KeyValue) (*api.Status, error) {
	return s.store.Set(ctx, req)
}

func (s *KeyValueService) Delete(ctx context.Context, req *api.Key) (*api.Status, error) {
	return s.store.Delete(ctx, req)
}
