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

func (d *partitionDrainer) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Set(cctx, req)
}

func (d *partitionDrainer) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Get(cctx, req)
}

func (d *partitionDrainer) Delete(ctx context.Context, req *data.DeleteRequest) (*data.DeleteResponse, error) {
	cctx, cancel := d.drainer.WithDrain(ctx)
	defer cancel()
	return d.inner.Delete(cctx, req)
}
