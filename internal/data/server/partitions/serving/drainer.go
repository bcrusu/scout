package serving

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/shared"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ utils.Lifecycle    = (*partitionDrainer)(nil)
	_ data.ServiceServer = (*partitionDrainer)(nil)
)

type partitionDrainer struct {
	data.UnsafeServiceServer
	inner   service
	drainer *utils.Drainer
}

type service interface {
	shared.Service
	utils.Lifecycle
}

func newPartitionDrainer(inner service) *partitionDrainer {
	return &partitionDrainer{
		inner: inner,
	}
}

func (d *partitionDrainer) Start(ctx context.Context) error {
	if err := d.inner.Start(ctx); err != nil {
		return err
	}

	d.drainer = utils.NewDrainer(ctx)
	return nil
}

func (d *partitionDrainer) Stop() {
	d.drainer.Stop()
	d.inner.Stop()
}

func (d *partitionDrainer) IsLeader() bool {
	return d.inner.IsLeader()
}

func (d *partitionDrainer) Autocommit(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Autocommit(cctx, txn)
}

func (d *partitionDrainer) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Prepare(cctx, req)
}

func (d *partitionDrainer) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Commit(cctx, req)
}

func (d *partitionDrainer) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Abort(cctx, req)
}

func (d *partitionDrainer) StoreDecision(ctx context.Context, dec *data.TxnDecision) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.StoreDecision(cctx, dec)
}
