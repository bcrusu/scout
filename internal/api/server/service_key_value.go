package server

import (
	"context"

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
}

// NewKeyValueService returns a new KeyValueService instance
func NewKeyValueService() *KeyValueService {
	return &KeyValueService{}
}

func (s *KeyValueService) RegisterToServer(server *grpc.Server) {
	api.RegisterKeyValueServer(server, s)
}

// TODO
func (s *KeyValueService) Get(context.Context, *api.GetRequest) (*api.GetResponse, error) {
	return nil, nil
}

func (s *KeyValueService) Set(context.Context, *api.SetRequest) (*api.Status, error) {
	return nil, nil
}

func (s *KeyValueService) Delete(context.Context, *api.DeleteRequest) (*api.Status, error) {
	return nil, nil
}
