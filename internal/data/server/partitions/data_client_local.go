package partitions

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"google.golang.org/grpc"
)

var (
	_ data.ServiceClient = (*dataClientLocal)(nil)
)

// dataClientLocal routes local requests.
type dataClientLocal struct {
	controller *Controller
	client     data.ServiceClient
}

func (c *dataClientLocal) Autocommit(ctx context.Context, txn *data.Txn, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	if s, ok := c.controller.GetServiceForPartition(txn.Id.PrincipalPid); ok { //TODO must be leader
		return s.Autocommit(ctx, txn)
	}
	return c.client.Autocommit(ctx, txn, opts...)
}

func (c *dataClientLocal) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	if s, ok := c.controller.GetServiceForPartition(req.ParticipantPid); ok {
		return s.Prepare(ctx, req)
	}
	return c.client.Prepare(ctx, req, opts...)
}

func (c *dataClientLocal) Commit(ctx context.Context, req *data.CommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	if s, ok := c.controller.GetServiceForPartition(req.ParticipantPid); ok {
		return s.Commit(ctx, req)
	}
	return c.client.Commit(ctx, req, opts...)
}

func (c *dataClientLocal) Abort(ctx context.Context, req *data.AbortRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	if s, ok := c.controller.GetServiceForPartition(req.ParticipantPid); ok {
		return s.Abort(ctx, req)
	}
	return c.client.Abort(ctx, req, opts...)
}

func (c *dataClientLocal) StoreDecision(ctx context.Context, dec *data.TxnDecision, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	if s, ok := c.controller.GetServiceForPartition(dec.Id.PrincipalPid); ok {
		return s.StoreDecision(ctx, dec)
	}
	return c.client.StoreDecision(ctx, dec, opts...)
}
