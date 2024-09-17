package server

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/rpc"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service = (*DataService)(nil)
)

// DataService represents the data service.
type DataService struct {
	data.UnimplementedServiceServer
	controller *partitions.Controller
}

// NewDataService returns a new DataService instance
func NewDataService(controller *partitions.Controller) *DataService {
	return &DataService{
		controller: controller,
	}
}

func (s *DataService) RegisterToServer(server *grpc.Server) {
	data.RegisterServiceServer(server, s)
}

// TODO: compute partition id to route the request
func (s *DataService) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	if partition, ok := s.controller.GetPartition(0); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Set(ctx, req)
	}
}

func (s *DataService) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	if partition, ok := s.controller.GetPartition(0); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Get(ctx, req)
	}
}

func (s *DataService) Del(ctx context.Context, req *data.DelRequest) (*data.DelResponse, error) {
	if partition, ok := s.controller.GetPartition(0); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Del(ctx, req)
	}
}
