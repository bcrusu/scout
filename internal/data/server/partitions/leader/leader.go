package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/common"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/hlc"
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

// TODO: request validation
func (n *Leader) ExecuteTxnBatch(ctx context.Context, batch *data.TxnBatch) (*data.TxnBatchStatus, error) {
	cmd := &storage.ExecuteTxnBatch{
		Timestamp:       hlc.Now(),
		Autocommit:      batch.Autocommit,
		TwoPhasePrepare: batch.TwoPhasePrepare,
		TwoPhaseCommit:  batch.TwoPhaseCommit,
		TwoPhaseAbort:   batch.TwoPhaseAbort,
	}

	result, err := n.store.ExecuteTxnBatch(cmd)
	if err != nil {
		return nil, err
	}

	return &data.TxnBatchStatus{
		Status: result.Status,
	}, nil
}
