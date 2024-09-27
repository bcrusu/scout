package client

import "context"

type ctxKeyRoutingInfo struct{}

type routing struct {
	partitionID uint32
	replicaRead bool
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
