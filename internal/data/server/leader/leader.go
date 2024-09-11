package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/common"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   data.ServiceServer = (*Leader)(nil)
	_   utils.Lifecycle    = (*Leader)(nil)
	log                    = logging.WithComponent("data_leader")
)

// Leader implements the Leader role.
type Leader struct {
	data.UnsafeServiceServer
	*common.Shared
	store storage.Store
}

func New(store storage.Store) *Leader {
	return &Leader{
		Shared: common.New(),
		store:  store,
	}
}

func (n *Leader) Start(ctx context.Context) error {
	log.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop(ctx context.Context) {
	log.Debug(ctx, "Stopped leader")
}

func (n *Leader) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	payload := &storage.Set{
		Key:   req.Key,
		Value: req.Value,
	}

	if err := n.store.Set(payload); err != nil {
		return nil, err
	}

	return &data.SetResponse{}, nil
}

func (n *Leader) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	value, ok := n.store.Get(req.Key)
	if !ok {
		return nil, errors.NotFound
	}

	return &data.GetResponse{
		Value: value,
	}, nil
}

func (n *Leader) Del(ctx context.Context, req *data.DelRequest) (*data.DelResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	payload := &storage.Delete{
		Key: req.Key,
	}

	if err := n.store.Del(payload); err != nil {
		return nil, err
	}

	return &data.DelResponse{}, nil
}
