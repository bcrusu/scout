package utils

import (
	"context"
	"time"
)

// DebounceChan wraps the channel to debounce its output and emits only when it observes a pause in activity.
// The result chan will be closed when the source is closed or the context is canceled.
func DebounceChan[T any](ctx context.Context, source <-chan T, pause time.Duration) <-chan T {
	if pause < 0 {
		pause = 0
	}

	result := make(chan T, cap(source))
	send := func(t T) bool {
		select {
		case result <- t:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(result)
		var timer *time.Timer
		var last *T

		for {
			select {
			case x, ok := <-source:
				if !ok {
					if last != nil {
						timer.Stop()
						send(*last)
					}
					return
				}

				last = &x

				if timer == nil {
					timer = time.NewTimer(pause)
				} else {
					timer.Reset(pause)
				}
			case <-GetTimerChan(timer):
				if !send(*last) {
					return
				}
				last = nil
				timer.Stop()
			case <-ctx.Done():
				return
			}
		}
	}()

	return result
}

// MakeDebounceChan will make a new souce chan along with a debounced counterpart.
func MakeDebounceChan[T any](ctx context.Context, pause time.Duration, bufferSize ...int) (chan<- T, <-chan T) {
	size := GetOptionalParameter(0, bufferSize)
	source := make(chan T, size)
	debounced := DebounceChan(ctx, source, pause)
	return source, debounced
}
