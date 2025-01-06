package serving

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ utils.Lifecycle    = (*drainer)(nil)
	_ data.ServiceServer = (*drainer)(nil)
)

type drainer struct {
	data.UnsafeServiceServer
	inner   service
	log     logging.Logger
	drainer *utils.Drainer
}

type service interface {
	shared.Service
	utils.Lifecycle
}

func newDrainer(inner service, log logging.Logger) *drainer {
	return &drainer{
		inner: inner,
		log:   log,
	}
}

func (d *drainer) Start(ctx context.Context) error {
	if err := d.inner.Start(ctx); err != nil {
		return err
	}

	d.drainer = utils.NewDrainer(d.log)
	return nil
}

func (d *drainer) Stop() {
	d.drainer.Stop()
	d.inner.Stop()
}

func (d *drainer) IsLeader() bool {
	return d.inner.IsLeader()
}

func (d *drainer) Autocommit(ctx context.Context, req *data.AutocommitRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Autocommit(cctx, req)
}

func (d *drainer) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Prepare(cctx, req)
}

func (d *drainer) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Commit(cctx, req)
}

func (d *drainer) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Abort(cctx, req)
}

func (d *drainer) StoreDecision(ctx context.Context, dec *data.Decision) (*data.TxnStatus, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.StoreDecision(cctx, dec)
}

func (d *drainer) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
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
