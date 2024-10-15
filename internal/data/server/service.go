package server

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service          = (*DataService)(nil)
	_ data.ServiceServer   = (*DataService)(nil)
	_ txn.TxnServiceServer = (*DataService)(nil)
)

// DataService represents the data service.
type DataService struct {
	data.UnsafeServiceServer
	txn.UnsafeTxnServiceServer
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
	txn.RegisterTxnServiceServer(server, s)
}

func (s *DataService) Autocommit(ctx context.Context, req *txn.AutocommitRequest) (*txn.Status, error) {
	if partition, ok := s.controller.GetService(req.PartitionId); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Autocommit(ctx, req)
	}
}

func (s *DataService) Prepare(ctx context.Context, req *txn.PrepareRequest) (*txn.Status, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Prepare(ctx, req)
	}
}

func (s *DataService) Commit(ctx context.Context, req *txn.CommitRequest) (*txn.Status, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Commit(ctx, req)
	}
}

func (s *DataService) Abort(ctx context.Context, req *txn.AbortRequest) (*txn.Status, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Abort(ctx, req)
	}
}

func (s *DataService) StoreDecision(ctx context.Context, dec *txn.Decision) (*txn.Status, error) {
	if partition, ok := s.controller.GetService(dec.Id.PrincipalPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.StoreDecision(ctx, dec)
	}
}

func (s *DataService) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	if partition, ok := s.controller.GetService(req.PartitionId); !ok {
		return errors.Unavailable
	} else {
		return partition.StreamPartition(req, stream)
	}
}
