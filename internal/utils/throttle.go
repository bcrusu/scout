package utils

import (
	"time"
)

// ThrottleChan wraps the channel to throttle its output to max once per provided interval.
func ThrottleChan[T any](ch <-chan T, interval time.Duration) <-chan T {
	if interval < 0 {
		interval = 0
	}

	result := make(chan T, cap(ch))

	go func() {
		var last time.Time

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

// MakeThrottleChan will make a new souce chan along with a throttled counterpart.
func MakeThrottleChan[T any](interval time.Duration, bufferSize ...int) (chan<- T, <-chan T) {
	size := 0
	if len(bufferSize) == 1 {
		size = bufferSize[0]
	} else if len(bufferSize) > 1 {
		panic("unexpected bufferSize parameter")
	}

	source := make(chan T, size)
	throttled := ThrottleChan(source, interval)
	return source, throttled
}
