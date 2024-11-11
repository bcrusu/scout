package txn

import (
	"github.com/bcrusu/scout/internal/metrics"
)

type managerMeters struct {
	Tracked          metrics.UpDownCounter
	Running          metrics.UpDownCounter
	Timedout         metrics.Counter
	LocksHeld        metrics.UpDownCounter
	LocksFailed      metrics.Counter
	ValidationFailed metrics.Counter
}

func newManagerMeters(_ uint32) managerMeters {
	return managerMeters{
		Tracked:          metrics.NewUpDownCounter("txn.rw.tracked"),
		Running:          metrics.NewUpDownCounter("txn.rw.running"),
		Timedout:         metrics.NewCounter("txn.rw.timedout"),
		LocksHeld:        metrics.NewUpDownCounter("txn.rw.locks.held"),
		LocksFailed:      metrics.NewCounter("txn.rw.locks.failed"),
		ValidationFailed: metrics.NewCounter("txn.rw.validation.failed"),
	}
}

type readerMeters struct {
	Prepared metrics.UpDownCounter
}

func newReaderMeters(_ uint32) readerMeters {
	return readerMeters{
		Prepared: metrics.NewUpDownCounter("txn.ro.prepared"),
	}
}
