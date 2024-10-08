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

// TODO: request validation
func (s *Store) Get(ctx context.Context, req *api.KeyAt) (*api.ValueAt, error) {
	var action *data.Action

	if req.AtTime == nil {
		action = txn.Read(keyValueKeyspace, req.Key)
	} else {
		action = txn.ReadAt(keyValueKeyspace, req.Key, req.AtTime.AsTime())
	}

	result, err := s.processor.Execute(ctx, req.Key, action)
	if err != nil {
		return nil, err
	}

	return s.getSingleValue(result)
}

func (s *Store) Set(ctx context.Context, req *api.KeyValue) (*api.Status, error) {
	data, err := utils.MarshalProto(req.Value)
	if err != nil {
		return nil, err
	}

	action := txn.Upsert(keyValueKeyspace, req.Key, data)

	result, err := s.processor.Execute(ctx, req.Key, action)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Store) Delete(ctx context.Context, req *api.Key) (*api.Status, error) {
	action := txn.Delete(keyValueKeyspace, req.Key)

	result, err := s.processor.Execute(ctx, req.Key, action)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Store) getSingleValue(r *txn.TxnResult) (*api.ValueAt, error) {
	if err := r.GetError(); err != nil {
		return nil, err
	} else if l := len(r.ActionStatus); l != 1 {
		return nil, errors.Errorf("txn%s expected sigle result, but got %d.", r.Id, l)
	}

	value := r.ActionStatus[0].Value

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
