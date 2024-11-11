package stats

import (
	"context"

	"google.golang.org/grpc/stats"
)

type clientHandler struct{}

// NewClientHandler creates a new client stats.Handler.
func NewClientHandler() stats.Handler {
	return &clientHandler{}
}

// TagConn can attach some information to the given context.
func (h *clientHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleRPC processes the RPC stats.
func (h *clientHandler) HandleConn(ctx context.Context, info stats.ConnStats) {}

// TagRPC can attach some information to the given context.
func (h *clientHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	rctx := &rpcContext{
		labels: getLabels(info.FullMethodName),
	}
	return context.WithValue(ctx, ctxKey{}, rctx)
}

// HandleConn processes the Conn stats.
func (h *clientHandler) HandleRPC(ctx context.Context, stats stats.RPCStats) {
	handleRPC(ctx, stats, clientMeters)
}
