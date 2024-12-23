package utils

import (
	"math/rand/v2"
	"time"

	"golang.org/x/time/rate"
)

const (
	RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"
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

func NewTimer(d time.Duration, stopped bool) *time.Timer {
	timer := time.NewTimer(d)
	if stopped {
		timer.Stop()
	}
	return timer
}

func NewTicker(d time.Duration, stopped bool) *time.Ticker {
	timer := time.NewTicker(d)
	if stopped {
		timer.Stop()
	}
	return timer
}

func GetTimerChan(timer *time.Timer) <-chan time.Time {
	if timer != nil {
		return timer.C
	}
	return nil
}

func GetTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}
