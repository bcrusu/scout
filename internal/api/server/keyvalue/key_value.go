package keyvalue

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/api/server/txn"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Store struct {
	processor *txn.Processor
}

func NewStore(processor *txn.Processor) *Store {
	return &Store{
		processor: processor,
	}
}

// TODO: request validation
func (s *Store) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	var data []byte
	var ts time.Time
	var err error

	if req.AtTime == nil {
		data, ts, err = s.processor.Get(ctx, req.Key)
	} else {
		data, ts, err = s.processor.GetAt(ctx, req.Key, req.AtTime.AsTime())
	}

	if err != nil {
		return nil, err
	}

	value, err := utils.UnmarshalProto[api.Value](data)
	if err != nil {
		return nil, err
	}

	return &api.GetResponse{
		Value: value,
		Time:  timestamppb.New(ts),
	}, nil
}

func (s *Store) Set(ctx context.Context, req *api.SetRequest) (*api.Status, error) {
	data, err := utils.MarshalProto(req.Value)
	if err != nil {
		return nil, err
	}

	// TODO: keyspace allocation
	action := txn.Upsert(0, req.Key, data)

	status, err := s.processor.ExecuteSingle(ctx, req.Key, action)
	if err != nil {
		return nil, err
	}

	//  TODO: check status.State/ActionError

	return &api.Status{
		Time: hlc.AsTimestamp(status.Timestamp),
	}, nil
}

func (s *Store) Delete(ctx context.Context, req *api.DeleteRequest) (*api.Status, error) {
	action := txn.Delete(0, req.Key)

	status, err := s.processor.ExecuteSingle(ctx, req.Key, action)
	if err != nil {
		return nil, err
	}

	return &api.Status{
		Time: hlc.AsTimestamp(status.Timestamp),
	}, nil
}
