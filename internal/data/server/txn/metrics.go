package txn

import (
	"github.com/bcrusu/scout/internal/metrics"
)

type managerMetrics struct {
	Tracked          metrics.UpDownCounter
	Autocommitted    metrics.Counter
	Running          metrics.UpDownCounter
	Prepared         metrics.Counter
	Committed        metrics.Counter
	Aborted          metrics.Counter
	Decided          metrics.Counter
	Timedout         metrics.Counter
	LocksHeld        metrics.UpDownCounter
	LocksFailed      metrics.Counter
	ValidationFailed metrics.Counter
}

func newManagerMetrics(_ uint32) managerMetrics {
	return managerMetrics{
		Tracked:          metrics.NewUpDownCounter("txn.rw.tracked"),
		Autocommitted:    metrics.NewCounter("txn.rw.autocommitted"),
		Running:          metrics.NewUpDownCounter("txn.rw.running"),
		Prepared:         metrics.NewCounter("txn.rw.prepared"),
		Committed:        metrics.NewCounter("txn.rw.committed"),
		Aborted:          metrics.NewCounter("txn.rw.aborted"),
		Decided:          metrics.NewCounter("txn.rw.decided"),
		Timedout:         metrics.NewCounter("txn.rw.timedout"),
		LocksHeld:        metrics.NewUpDownCounter("txn.rw.locks.held"),
		LocksFailed:      metrics.NewCounter("txn.rw.locks.failed"),
		ValidationFailed: metrics.NewCounter("txn.rw.validation.failed"),
	}
}

type readerMetrics struct {
	Prepared metrics.UpDownCounter
}

func newReaderMetrics(_ uint32) readerMetrics {
	return readerMetrics{
		Prepared: metrics.NewUpDownCounter("txn.ro.prepared"),
	}
}
