package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/common"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ data.ServiceServer = (*Leader)(nil)
	_ utils.Lifecycle    = (*Leader)(nil)
)

// Leader implements the Leader role.
type Leader struct {
	data.UnsafeServiceServer
	*common.Shared
	log   logging.Logger
	store storage.Store
}

func New(partitionID uint32, store storage.Store) *Leader {
	return &Leader{
		Shared: common.New(),
		log:    logging.WithComponent("leader").With("partition", partitionID),
		store:  store,
	}
}

func (n *Leader) Start(ctx context.Context) error {
	n.log.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop() {
	n.log.NoContext().Debug("Stopped leader")
}

func (n *Leader) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	payload := &storage.Set{
		Keyspace: req.Keyspace,
		Key:      req.Key,
		Value:    req.Value,
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

	value, ok := n.store.Get(req.Keyspace, req.Key)
	if !ok {
		return nil, errors.NotFound
	}

	return &data.GetResponse{
		Value: value,
	}, nil
}

func (n *Leader) Delete(ctx context.Context, req *data.DeleteRequest) (*data.DeleteResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	payload := &storage.Delete{
		Keyspace: req.Keyspace,
		Key:      req.Key,
	}

	if err := n.store.Delete(payload); err != nil {
		return nil, err
	}

	return &data.DeleteResponse{}, nil
}
