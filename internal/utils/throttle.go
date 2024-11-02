package utils

import (
	"context"
	"time"
)

// ThrottleChan wraps the channel to throttle its output to max once per provided interval.
// The result chan will be closed when the source is closed or the context is canceled.
func ThrottleChan[T any](ctx context.Context, ch <-chan T, interval time.Duration) <-chan T {
	if interval < 0 {
		interval = 0
	}

	result := make(chan T, cap(ch))

	go func() {
		var last time.Time

		for {
			select {
			case x, ok := <-ch:
				if !ok {
					return
				}

				now := time.Now()
				if last.Before(now.Add(-interval)) {
					last = now
					result <- x
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return result
}

// MakeThrottleChanContext will make a new souce chan along with a throttled counterpart.
func MakeThrottleChanContext[T any](ctx context.Context, interval time.Duration, bufferSize ...int) (chan<- T, <-chan T) {
	size := GetOptionalParameter(0, bufferSize)
	source := make(chan T, size)
	throttled := ThrottleChan(ctx, source, interval)
	return source, throttled
}

// MakeThrottleChan will make a new souce chan along with a throttled counterpart.
func MakeThrottleChan[T any](interval time.Duration, bufferSize ...int) (chan<- T, <-chan T) {
	return MakeThrottleChanContext[T](context.Background(), interval, bufferSize...)
}
