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
	_ rpc.Service        = (*DataService)(nil)
	_ data.ServiceServer = (*DataService)(nil)
)

// DataService represents the data service.
type DataService struct {
	data.UnsafeServiceServer
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

func (s *DataService) Autocommit(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	if partition, ok := s.controller.GetService(txn.Id.PrincipalPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Autocommit(ctx, txn)
	}
}

func (s *DataService) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Prepare(ctx, req)
	}
}

func (s *DataService) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Commit(ctx, req)
	}
}

func (s *DataService) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	if partition, ok := s.controller.GetService(req.ParticipantPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.Abort(ctx, req)
	}
}

func (s *DataService) StoreDecision(ctx context.Context, dec *data.TxnDecision) (*data.TxnStatus, error) {
	if partition, ok := s.controller.GetService(dec.Id.PrincipalPid); !ok {
		return nil, errors.Unavailable
	} else {
		return partition.StoreDecision(ctx, dec)
	}
}
