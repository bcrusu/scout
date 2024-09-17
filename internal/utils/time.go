package utils

import (
	"math/rand/v2"
	"time"
)

// AddJitter adds random jitter in the range (-pct, +pct).
func AddJitter(d time.Duration, pct float64) time.Duration {
	if pct == 0 {
		return d
	}

	jitter := float64(d) * pct * (rand.Float64()*2 - 1)
	d += time.Duration(jitter)

	if d < 0 {
		return 0
	}
	return d
}
