package client

import "context"

type ctxKeyRoutingInfo struct{}
type ctxKeyPreferredServer struct{}

type routing struct {
	partitionID uint32
	replicaRead bool
}

type preferredServer struct {
	serverID uint64
	enforce  bool
}

func withRouting(ctx context.Context, r routing) context.Context {
	return context.WithValue(ctx, ctxKeyRoutingInfo{}, r)
}

func getRouting(ctx context.Context) (routing, bool) {
	v := ctx.Value(ctxKeyRoutingInfo{})
	if v == nil {
		return routing{}, false
	}

	return v.(routing), true
}

// WithPreferredServer allows callers to select a preferred server to
// route the request to. To note that, the partition routing rules still
// apply and, the request will fail with status code Unavailable if the
// preferred server does not match the request type. For example: the
// preferred server does not currently serve the requested partition, or
// a non-replica read tries to invoke a follower server, etc.
func WithPreferredServer(ctx context.Context, serverID uint64, enforce bool) context.Context {
	p := preferredServer{
		serverID: serverID,
		enforce:  enforce,
	}

	return context.WithValue(ctx, ctxKeyPreferredServer{}, p)
}

func getPreferredServer(ctx context.Context) (preferredServer, bool) {
	v := ctx.Value(ctxKeyPreferredServer{})
	if v == nil {
		return preferredServer{}, false
	}

	return v.(preferredServer), true
}
