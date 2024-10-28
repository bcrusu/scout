package serving

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ utils.Lifecycle      = (*partitionDrainer)(nil)
	_ data.ServiceServer   = (*partitionDrainer)(nil)
	_ txn.TxnServiceServer = (*partitionDrainer)(nil)
)

type partitionDrainer struct {
	data.UnsafeServiceServer
	txn.UnsafeTxnServiceServer
	inner   service
	log     logging.Logger
	drainer *utils.Drainer
}

type service interface {
	shared.Service
	utils.Lifecycle
}

func newPartitionDrainer(inner service, log logging.Logger) *partitionDrainer {
	return &partitionDrainer{
		inner: inner,
		log:   log,
	}
}

func (d *partitionDrainer) Start(ctx context.Context) error {
	if err := d.inner.Start(ctx); err != nil {
		return err
	}

	d.drainer = utils.NewDrainer(ctx, d.log)
	return nil
}

func (d *partitionDrainer) Stop() {
	d.drainer.Stop()
	d.inner.Stop()
}

func (d *partitionDrainer) IsLeader() bool {
	return d.inner.IsLeader()
}

func (d *partitionDrainer) Autocommit(ctx context.Context, req *txn.AutocommitRequest) (*txn.Status, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Autocommit(cctx, req)
}

func (d *partitionDrainer) Prepare(ctx context.Context, req *txn.PrepareRequest) (*txn.Status, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Prepare(cctx, req)
}

func (d *partitionDrainer) Commit(ctx context.Context, req *txn.CommitRequest) (*txn.Status, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Commit(cctx, req)
}

func (d *partitionDrainer) Abort(ctx context.Context, req *txn.AbortRequest) (*txn.Status, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Abort(cctx, req)
}

func (d *partitionDrainer) StoreDecision(ctx context.Context, dec *txn.Decision) (*txn.Status, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.StoreDecision(cctx, dec)
}

func (d *partitionDrainer) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	cctx, cancel := d.drainer.WithDrain(stream.Context())
	defer cancel()

	sw := &streamWrapper{stream, cctx}
	return d.inner.StreamPartition(req, sw)
}

type streamWrapper struct {
	grpc.ServerStreamingServer[data.StreamResponse]
	ctx context.Context
}

func (s *streamWrapper) Context() context.Context {
	return s.ctx
}
