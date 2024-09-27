package server

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/keyvalue"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service = (*KeyValueService)(nil)
)

// KeyValueService represents the key-value service.
type KeyValueService struct {
	api.UnimplementedKeyValueServer
	store keyvalue.Store
}

// NewKeyValueService returns a new KeyValueService instance
func NewKeyValueService(store keyvalue.Store) *KeyValueService {
	return &KeyValueService{
		store: store,
	}
}

func (s *KeyValueService) RegisterToServer(server *grpc.Server) {
	api.RegisterKeyValueServer(server, s)
}

func (s *KeyValueService) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	return s.store.Get(ctx, req)
}

func (s *KeyValueService) Set(ctx context.Context, req *api.SetRequest) (*api.Status, error) {
	return s.store.Set(ctx, req)
}

func (s *KeyValueService) Delete(ctx context.Context, req *api.DeleteRequest) (*api.Status, error) {
	return s.store.Delete(ctx, req)
}
