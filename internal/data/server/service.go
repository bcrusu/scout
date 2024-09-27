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

func (s *DataService) ExecuteTxnBatch(ctx context.Context, batch *data.TxnBatch) (*data.TxnBatchStatus, error) {
	if partition, ok := s.controller.GetServiceForPartition(batch.PartitionId); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.ExecuteTxnBatch(ctx, batch)
	}
}
