package partitions

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"google.golang.org/grpc"
)

var (
	_ client.DataClient = (*dataClientLocal)(nil)
)

// dataClientLocal routes local requests.
type dataClientLocal struct {
	controller *Controller
	client     client.DataClient
}

func (c *dataClientLocal) Autocommit(ctx context.Context, req *txn.AutocommitRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	if s, ok := c.controller.GetService(req.PartitionId); ok && (s.IsLeader() || req.IsSnapshotRead()) {
		return s.Autocommit(ctx, req)
	}
	return c.client.Autocommit(ctx, req, opts...)
}

func (c *dataClientLocal) Prepare(ctx context.Context, req *txn.PrepareRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	if s, ok := c.controller.GetService(req.ParticipantPid); ok && s.IsLeader() {
		return s.Prepare(ctx, req)
	}
	return c.client.Prepare(ctx, req, opts...)
}

func (c *dataClientLocal) Commit(ctx context.Context, req *txn.CommitRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	if s, ok := c.controller.GetService(req.ParticipantPid); ok && s.IsLeader() {
		return s.Commit(ctx, req)
	}
	return c.client.Commit(ctx, req, opts...)
}

func (c *dataClientLocal) Abort(ctx context.Context, req *txn.AbortRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	if s, ok := c.controller.GetService(req.ParticipantPid); ok && s.IsLeader() {
		return s.Abort(ctx, req)
	}
	return c.client.Abort(ctx, req, opts...)
}

func (c *dataClientLocal) StoreDecision(ctx context.Context, dec *txn.Decision, opts ...grpc.CallOption) (*txn.Status, error) {
	if s, ok := c.controller.GetService(dec.Id.PrincipalPid); ok && s.IsLeader() {
		return s.StoreDecision(ctx, dec)
	}
	return c.client.StoreDecision(ctx, dec, opts...)
}

func (c *dataClientLocal) StreamPartition(ctx context.Context, req *data.StreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[data.StreamResponse], error) {
	return c.client.StreamPartition(ctx, req, opts...)
}

func (c *dataClientLocal) Start(ctx context.Context) error {
	return c.client.Start(ctx)
}

func (c *dataClientLocal) Stop() {
	c.client.Stop()
}
