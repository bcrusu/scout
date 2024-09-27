package txn

import (
	"context"
	"sync"

	"github.com/bcrusu/graph/internal/data"
)

type batcher struct {
	config  Config
	lock    sync.Mutex
	current *batch
}

type batch struct {
	size        int
	actionCount int
	byPartition map[uint32]*data.TxnBatch
}

func newBatcher(config Config) *batcher {
	return &batcher{
		config:  config,
		current: newBatch(),
	}
}

func newBatch() *batch {
	return &batch{
		byPartition: map[uint32]*data.TxnBatch{},
	}
}

// TODO
func (b *batcher) executeSingle(ctx context.Context, partitionID uint32, txn *data.Txn) (*data.TxnStatus, error) {
	return nil, nil
}

func (b *batcher) executeMulti(ctx context.Context, multi *Multi) (*data.TxnBatchStatus, error) {
	return nil, nil
}
