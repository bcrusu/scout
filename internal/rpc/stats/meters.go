package stats

import (
	"fmt"

	"github.com/bcrusu/scout/internal/metrics"
)

var (
	serverMeters = newMeters("server")
	clientMeters = newMeters("client")
)

type meters struct {
	Duration      metrics.Histogram
	Started       metrics.Counter
	Completed     metrics.Counter
	MsgSentPerRPC metrics.Histogram
	MsgRecvPerRPC metrics.Histogram
}

func newMeters(role string) *meters {
	getName := func(format string) string {
		return fmt.Sprintf(format, role)
	}

	return &meters{
		Duration:      metrics.NewHistogram(getName("rpc.%s.duration")),
		Started:       metrics.NewCounter(getName("rpc.%s.started")),
		Completed:     metrics.NewCounter(getName("rpc.%s.completed")),
		MsgSentPerRPC: metrics.NewHistogram(getName("rpc.%s.msg_sent_per_rpc")),
		MsgRecvPerRPC: metrics.NewHistogram(getName("rpc.%s.msg_recv_per_rpc")),
	}
}
