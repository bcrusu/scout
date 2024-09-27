package partitions

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ utils.Lifecycle = (*partitionDrainer)(nil)
)

type partitionDrainer struct {
	data.UnimplementedServiceServer
	inner   data.ServiceServer
	drainer *utils.Drainer
}

func newPartitionDrainer(inner data.ServiceServer) *partitionDrainer {
	return &partitionDrainer{
		inner: inner,
	}
}

func (d *partitionDrainer) Start(ctx context.Context) error {
	d.drainer = utils.NewDrainer(ctx)
	return nil
}

func (d *partitionDrainer) Stop() {
	d.drainer.Stop()
}

func (d *partitionDrainer) ExecuteTxnBatch(ctx context.Context, batch *data.TxnBatch) (*data.TxnBatchStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.ExecuteTxnBatch(cctx, batch)
}
