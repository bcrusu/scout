package utils

// #include <time.h>
import "C"

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"golang.org/x/time/rate"
)

const (
	RFC3339Milli     = "2006-01-02T15:04:05.999Z07:00"
	xCLOCK_REALTIME  = 0
	xCLOCK_MONOTONIC = 1
)

// AddJitter adds random jitter in the range (-pct, +pct).
// If pct is not provided, will use 0.15 as the default value.
func AddJitter(d time.Duration, pct ...float64) time.Duration {
	p := GetOptionalParameter(0.15, pct)
	if p > 1 {
		p = 1
	} else if p <= 0 {
		return d
	}

	jitter := float64(d) * p * (rand.Float64()*2 - 1)
	d += time.Duration(jitter)

	if d < 0 {
		return 0
	}
	return d
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(limit int, interval time.Duration) *rate.Limiter {
	perSecond := rate.Limit(float64(limit) / interval.Seconds())
	return rate.NewLimiter(perSecond, limit)
}

// NewTicker creates a new stopped timer.
func NewTimer(d time.Duration) *time.Timer {
	timer := time.NewTimer(d)
	timer.Stop()
	return timer
}

// NewTicker creates a new stopped ticker.
func NewTicker(d time.Duration) *time.Ticker {
	timer := time.NewTicker(d)
	timer.Stop()
	return timer
}

// GetTimerChan returns the timer.C chan if timer is not nil.
func GetTimerChan(timer *time.Timer) <-chan time.Time {
	if timer != nil {
		return timer.C
	}
	return nil
}

// GetTickerChan returns the ticker.C chan if ticker is not nil.
func GetTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}

// GetMonoTime returns current monotonic system time.
func GetMonoTime() time.Time {
	return getTime(xCLOCK_MONOTONIC)
}

// SetTime sets the system time.
func SetTime(value time.Time) error {
	nano := value.UnixNano()

	ts := &C.struct_timespec{
		(C.__time_t)(nano / 1e9),
		(C.__syscall_slong_t)(nano % 1e9),
	}

	errC := C.clock_settime((C.clockid_t)(xCLOCK_REALTIME), ts)
	if errC != 0 {
		return errors.Errorf("clock_settime call failed with code %d", errC)
	}

	return nil
}

func getTime(clockid int) time.Time {
	var ts C.struct_timespec

	errC := C.clock_gettime((C.clockid_t)(clockid), &ts)
	if errC != 0 {
		panic(fmt.Sprintf("clock_gettime call failed with code %d", errC))
	}

	value := time.Unix(int64(ts.tv_sec), int64(ts.tv_nsec))
	return value
}
