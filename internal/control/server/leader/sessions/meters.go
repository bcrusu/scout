package sessions

import (
	"sync/atomic"

	"github.com/bcrusu/scout/internal/metrics"
)

type trackerMeters struct {
	SessionCount      atomic.Int64
	MsgReceiveSuccess atomic.Int64
	MsgReceiveError   atomic.Int64
	MsgReceiveDropped atomic.Int64
	MsgSendSuccess    atomic.Int64
	MsgSendError      atomic.Int64
	MsgSendDropped    atomic.Int64
	Unregister        metrics.Unregister
}

func newTrackerMeters() *trackerMeters {
	m := &trackerMeters{}

	m.Unregister = metrics.UnregisterAll(
		metrics.ObserveAtomicInt64("sessions.count", &m.SessionCount),
		metrics.ObserveAtomicInt64("sessions.msg.receive.success", &m.MsgReceiveSuccess),
		metrics.ObserveAtomicInt64("sessions.msg.receive.error", &m.MsgReceiveError),
		metrics.ObserveAtomicInt64("sessions.msg.receive.dropped", &m.MsgReceiveDropped),
		metrics.ObserveAtomicInt64("sessions.msg.send.success", &m.MsgSendSuccess),
		metrics.ObserveAtomicInt64("sessions.msg.send.error", &m.MsgReceiveError),
		metrics.ObserveAtomicInt64("sessions.msg.send.dropped", &m.MsgReceiveDropped))

	return m
}
