package stats

import (
	"context"

	"google.golang.org/grpc/stats"
)

type serverHandler struct{}

// NewServerHandler creates a new server stats.Handler.
func NewServerHandler() stats.Handler {
	return &serverHandler{}
}

// TagConn can attach some information to the given context.
func (h *serverHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleRPC processes the RPC stats.
func (h *serverHandler) HandleConn(ctx context.Context, info stats.ConnStats) {}

// TagRPC can attach some information to the given context.
func (h *serverHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	rctx := &rpcContext{
		labels: getLabels(info.FullMethodName),
	}
	return context.WithValue(ctx, ctxKey{}, rctx)
}

// HandleConn processes the Conn stats.
func (h *serverHandler) HandleRPC(ctx context.Context, stats stats.RPCStats) {
	handleRPC(ctx, stats, serverMeters)
}
