package events

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/utils"
)

var (
	globalInstance = NewMessageBus()
)

// SetMessageBus allows changing the current MessageBus instance.
func SetMessageBus(bus *MessageBus) {
	globalInstance = bus
}

// Subscribe returns a Subscriber instance for a specific message type.
func Subscribe[T any](bufferSize ...int) *Subscriber[T] {
	size := utils.GetOptionalParameter(1, bufferSize)
	if size < 1 {
		size = 1
	}
	return subscribeAt[T](globalInstance, size)
}

// SubscribeDebounced is like Subscribe, but the observed messages will be debounced.
func SubscribeDebounced[T any](ctx context.Context, debounce time.Duration, bufferSize ...int) *Subscriber[T] {
	size := utils.GetOptionalParameter(1, bufferSize)
	if size < 1 {
		size = 1
	}
	return subscribeDebouncedAt[T](globalInstance, ctx, debounce, size)
}

// SubscribeThrottled is like Subscribe, but the observed messages will be throttled.
func SubscribeThrottled[T any](ctx context.Context, throttle time.Duration, bufferSize ...int) *Subscriber[T] {
	size := utils.GetOptionalParameter(1, bufferSize)
	if size < 1 {
		size = 1
	}
	return subscribeThrottledAt[T](globalInstance, ctx, throttle, size)
}

// Publish sends the message to the current subscribers for that message
// type. It blocks waiting for all subscribers to receive the message.
func Publish[T any](msg T) {
	publishAt[T](globalInstance, msg)
}

// TryPublish is similar to Publish, but it will skip subscribers that
// are not ready. Returns true if all subscribers received the message.
func TryPublish[T any](msg T) bool {
	return tryPublishAt[T](globalInstance, msg)
}
