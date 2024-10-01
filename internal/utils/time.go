package utils

import (
	"math/rand/v2"
	"time"

	"golang.org/x/time/rate"
)

// AddJitter adds random jitter in the range (-pct, +pct).
func AddJitter(d time.Duration, pct float64) time.Duration {
	if pct <= 0 {
		return d
	}

	jitter := float64(d) * pct * (rand.Float64()*2 - 1)
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
