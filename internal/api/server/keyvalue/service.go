package keyvalue

import (
	"context"

	"github.com/bcrusu/scout/internal/api/server/txn"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/pkg/keyvalue"
	"google.golang.org/grpc"
)

const (
	// TODO: keyspace allocation
	keyValueKeyspace = 100
)

var (
	_ rpc.Service            = (*Service)(nil)
	_ keyvalue.ServiceServer = (*Service)(nil)
)

type Service struct {
	keyvalue.UnsafeServiceServer
	processor *txn.Processor
}

func NewService(processor *txn.Processor) *Service {
	return &Service{
		processor: processor,
	}
}

func (s *Service) RegisterToServer(server *grpc.Server) {
	keyvalue.RegisterServiceServer(server, s)
}

func (s *Service) Get(ctx context.Context, req *keyvalue.GetRequest) (*keyvalue.GetResponse, error) {
	t := s.processor.New().SnapshotReadAtTimestamp(req.Snapshot)
	actionIDs := make([]uint32, len(req.Keys))

	for i, key := range req.Keys {
		action := txn.Read(keyValueKeyspace, key)
		t.Append(key, action)
		actionIDs[i] = action.Id
	}

	result, err := s.processor.Process(ctx, t)
	if err != nil {
		return nil, err
	}

	resp := &keyvalue.GetResponse{
		Values:    make([][]byte, len(req.Keys)),
		Timestamp: hlc.AsTimestamp(result.Timestamp),
	}

	for i, id := range actionIDs {
		value, err := s.getSingleResult(result, id)
		if err != nil {
			return nil, err
		}

		resp.Values[i] = value
	}

	return resp, nil
}

func (s *Service) Set(ctx context.Context, req *keyvalue.SetRequest) (*keyvalue.Status, error) {
	t := s.processor.New()

	for _, kv := range req.Items {
		t.Append(kv.Key, txn.Upsert(keyValueKeyspace, kv.Key, kv.Value))
	}

	result, err := s.processor.Process(ctx, t)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Service) Delete(ctx context.Context, req *keyvalue.DeleteRequest) (*keyvalue.Status, error) {
	t := s.processor.New()

	for _, key := range req.Keys {
		t.Append(key, txn.Delete(keyValueKeyspace, key))
	}

	result, err := s.processor.Process(ctx, t)
	if err != nil {
		return nil, err
	}

	return s.getStatus(result)
}

func (s *Service) getSingleResult(r *txn.TxnResult, actionID uint32) ([]byte, error) {
	status, ok := r.ActionStatus[actionID]

	switch {
	case !ok:
		return nil, errors.Errorf("txn %s action status not found.", r.Id)
	case len(status.Results) > 1:
		return nil, errors.Errorf("txn %s action status has too many %d results.", r.Id, len(status.Results))
	case status.Code == data.ActionStatus_KeyNotFound:
		return nil, nil
	case status.Code != data.ActionStatus_Success:
		return nil, status.Code.ToError()
	default:
		value := status.Results[0]

		switch x := value.Payload.(type) {
		case *data.Value_Bytes:
			return x.Bytes, nil
		default:
			return nil, errors.Errorf("unexpected value type %T", value.Payload)
		}
	}
}

func (s *Service) getStatus(r *txn.TxnResult) (*keyvalue.Status, error) {
	if err := r.GetFirstError(); err != nil {
		return nil, err
	}

	return &keyvalue.Status{
		Timestamp: hlc.AsTimestamp(r.Timestamp),
	}, nil
}
