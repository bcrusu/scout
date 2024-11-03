package keyvalue

import (
	"context"

	"github.com/bcrusu/scout/internal/api/server/txn"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/api"
)

const (
	// TODO: keyspace allocation
	keyValueKeyspace = 100
)

type Store struct {
	processor *txn.Processor
}

func NewStore(processor *txn.Processor) *Store {
	return &Store{
		processor: processor,
	}
}

func (s *Store) Get(ctx context.Context, req *api.KeyAt) (*api.ValueAt, error) {
	action := txn.Read(keyValueKeyspace, req.Key)

	txn := s.processor.New().
		Append(req.Key, action).
		SnapshotReadAtTimestamp(req.AtTime)

	result, err := s.processor.Process(ctx, txn)
	if err != nil {
		return nil, err
	}

	return s.getSingleValue(result, action.Id)
}

func (s *Store) Set(ctx context.Context, req *api.KeyValue) (*api.Status, error) {
	data, err := utils.MarshalProto(req.Value)
	if err != nil {
		return nil, err
	}

	txn := s.processor.New().Append(req.Key, txn.Upsert(keyValueKeyspace, req.Key, data))

	result, err := s.processor.Process(ctx, txn)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Store) Delete(ctx context.Context, req *api.Key) (*api.Status, error) {
	txn := s.processor.New().Append(req.Key, txn.Delete(keyValueKeyspace, req.Key))

	result, err := s.processor.Process(ctx, txn)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Store) getSingleValue(r *txn.TxnResult, actionID uint32) (*api.ValueAt, error) {
	if err := r.GetError(); err != nil {
		return nil, err
	}

	status, ok := r.ActionStatus[actionID]
	if !ok {
		return nil, errors.Errorf("txn %s action status not found.", r.Id)
	} else if l := len(status.Results); l != 1 {
		return nil, errors.Errorf("txn %s expected single action result, but got %d.", r.Id, l)
	}

	value := status.Results[0]

	switch x := value.Payload.(type) {
	case *data.Value_Bytes:
		value, err := utils.UnmarshalProto[api.Value](x.Bytes)
		if err != nil {
			return nil, err
		}

		return &api.ValueAt{
			Value:  value,
			AtTime: hlc.AsTimestamp(r.Timestamp),
		}, nil
	default:
		return nil, errors.Errorf("unexpected value type %T", value.Payload)
	}
}

func (s *Store) getStatus(r *txn.TxnResult) (*api.Status, error) {
	if err := r.GetError(); err != nil {
		return nil, err
	}

	return &api.Status{
		Time: hlc.AsTimestamp(r.Timestamp),
	}, nil
}
