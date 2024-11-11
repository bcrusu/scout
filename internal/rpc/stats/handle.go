package stats

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type ctxKey struct{}

type rpcContext struct {
	labels       metrics.Labels
	messagesRecv atomic.Int64
	messagesSent atomic.Int64
}

func handleRPC(ctx context.Context, rstats stats.RPCStats, meters *meters) {
	rctx, _ := ctx.Value(ctxKey{}).(*rpcContext)

	switch x := rstats.(type) {
	case *stats.Begin:
		meters.Started.Add(1, rctx.labels)
	case *stats.InPayload:
		rctx.messagesRecv.Add(1)
	case *stats.OutPayload:
		rctx.messagesSent.Add(1)
	case *stats.End:
		statusCode := codes.OK
		if x.Error != nil {
			s, _ := status.FromError(x.Error)
			statusCode = s.Code()
		}

		labels := rctx.labels.With("rpc.status_code", statusCode)

		meters.Duration.Record(int(x.EndTime.Sub(x.BeginTime).Milliseconds()), labels)
		meters.Completed.Add(1, labels)
		meters.MsgRecvPerRPC.Record(int(rctx.messagesRecv.Load()), labels)
		meters.MsgSentPerRPC.Record(int(rctx.messagesSent.Load()), labels)
	}
}

func getLabels(fullMethod string) metrics.Labels {
	fullMethod = strings.TrimPrefix(fullMethod, "/")

	var service string
	var method string

	if idx := strings.LastIndex(fullMethod, "/"); idx < 0 {
		service = "unknown"
		method = fullMethod
	} else {
		service, method = fullMethod[:idx], fullMethod[idx+1:]
	}

	return metrics.NewLabels("rpc.service", service, "rpc.method", method)
}
