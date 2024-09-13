package utils

import (
	"time"
)

// ThrottleChan wraps the channel to throttle its output to max once per provided interval.
func ThrottleChan[T any](ch <-chan T, interval time.Duration) <-chan T {
	result := make(chan T, cap(ch))
	var last time.Time

	go func() {
		for x := range ch {
			now := time.Now()
			if last.Before(now.Add(-interval)) {
				last = now
				result <- x
			}
		}

		close(result)
	}()

	return result
}
