package sessions

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
)

const (
	minTimeOffset        = time.Millisecond
	globalTimeOffsetPct  = float64(80)
	sessionTimeOffsetPct = float64(95)
	histoWarmupCount     = 100
)

// globalTimeOffset tracks the global, across all sessions, time offset.
type globalTimeOffset struct {
	lock  sync.Mutex
	histo *hdrhistogram.Histogram
}

// sessionTimeOffset tracks the time offset between the control plane leader and
// the connected server. If the offset exceeds the configured limit the session
// is terminated with TimeOffsetOutOfRange.
type sessionTimeOffset struct {
	maxTimeOffset time.Duration
	global        *globalTimeOffset
	histo         *hdrhistogram.Histogram
}

func newGlobalTimeOffset(maxTimeOffset time.Duration) *globalTimeOffset {
	return &globalTimeOffset{
		histo: hdrhistogram.New(int64(minTimeOffset), int64(maxTimeOffset), 2),
	}
}

func newSessionTimeOffset(global *globalTimeOffset, maxTimeOffset time.Duration) *sessionTimeOffset {
	return &sessionTimeOffset{
		maxTimeOffset: maxTimeOffset,
		global:        global,
		histo:         hdrhistogram.New(int64(minTimeOffset), int64(maxTimeOffset), 2),
	}
}

func (t *globalTimeOffset) record(offset time.Duration) time.Duration {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.histo.RecordValue(int64(offset))

	if t.histo.TotalCount() < histoWarmupCount {
		return time.Duration(t.histo.Max())
	} else {
		return time.Duration(t.histo.ValueAtPercentile(globalTimeOffsetPct))
	}
}

func (t *sessionTimeOffset) recordAndCheck(msg *control.TimestampResponse) error {
	offset := t.computeOffset(msg)

	switch {
	case offset < minTimeOffset:
		offset = minTimeOffset
	case offset > 2*t.maxTimeOffset:
		// don't even bother...
		return errors.TimeOffsetOutOfRange
	case offset > t.maxTimeOffset:
		// trim the value so the hdr histogram can accept it
		offset = t.maxTimeOffset
	}

	t.histo.RecordValue(int64(offset))

	sessionPct := time.Duration(t.histo.ValueAtPercentile(sessionTimeOffsetPct))
	globalPct := t.global.record(offset)

	if sessionPct > globalPct {
		return errors.TimeOffsetOutOfRange
	}

	return nil
}

// The offset is computed using the NTP clock synchronization algorithm
// formula: θ = 1/2 * [(t2 − t1) + (t3 − t4)], with the assumption that t2==t3.
func (t *sessionTimeOffset) computeOffset(msg *control.TimestampResponse) time.Duration {
	t1 := msg.RequestTimestamp.AsTime()
	t2 := msg.ResponseTimestamp.AsTime()
	t3 := t2
	t4 := time.Now()

	offset := (t2.Sub(t1) + t3.Sub(t4)) / 2

	if offset < 0 {
		offset = -offset
	}

	return offset
}
