package eventbus

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bcrusu/graph/internal/utils"
)

// MessageBus routes messages between participants.
type MessageBus struct {
	nextID  atomic.Uint64
	lock    sync.Mutex
	subs    map[uint64]any
	lastMsg map[reflect.Type]any
}

// Subscriber is the recipient of memssages.
type Subscriber[T any] struct {
	id           uint64
	chanIn       chan<- T
	chanOut      <-chan T
	bus          *MessageBus
	unsubscribed atomic.Bool
}

// NewMessageBus creates a new MessageBus.
func NewMessageBus() *MessageBus {
	return &MessageBus{
		subs:    map[uint64]any{},
		lastMsg: map[reflect.Type]any{},
	}
}

func addSubscriber[T any](bus *MessageBus, chanIn chan<- T, chanOut <-chan T) *Subscriber[T] {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	sub := &Subscriber[T]{
		id:      bus.nextID.Add(1),
		chanIn:  chanIn,
		chanOut: chanOut,
		bus:     bus,
	}

	// enqueue last message sent
	if lastMsg, ok := bus.lastMsg[reflect.TypeFor[T]()]; ok {
		sub.chanIn <- lastMsg.(T)
	}

	bus.subs[sub.id] = sub
	return sub
}

func subscribeAt[T any](bus *MessageBus, bufferSize int) *Subscriber[T] {
	ch := make(chan T, bufferSize)
	return addSubscriber(bus, ch, ch)
}

func subscribeDebouncedAt[T any](bus *MessageBus, ctx context.Context, debounce time.Duration, bufferSize int) *Subscriber[T] {
	in, out := utils.MakeDebounceChan[T](ctx, debounce, bufferSize)
	return addSubscriber(bus, in, out)
}

func subscribeThrottledAt[T any](bus *MessageBus, ctx context.Context, throttle time.Duration, bufferSize int) *Subscriber[T] {
	in, out := utils.MakeThrottleChanContext[T](ctx, throttle, bufferSize)
	return addSubscriber(bus, in, out)
}

func publishAt[T any](bus *MessageBus, msg T) {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.lastMsg[reflect.TypeFor[T]()] = msg

	for _, subAny := range bus.subs {
		if sub, ok := subAny.(*Subscriber[T]); ok {
			sub.chanIn <- msg
		}
	}
}

func tryPublishAt[T any](bus *MessageBus, msg T) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.lastMsg[reflect.TypeFor[T]()] = msg

	all := false
	for _, subAny := range bus.subs {
		if sub, ok := subAny.(*Subscriber[T]); ok {
			select {
			case sub.chanIn <- msg:
			default:
				all = false
			}
		}
	}
	return all
}

func (s *Subscriber[T]) Items() <-chan T {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	return s.chanOut
}

func (s *Subscriber[T]) Unsubscribe() {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	s.unsubscribed.Store(true)
	close(s.chanIn)

	s.bus.lock.Lock()
	delete(s.bus.subs, s.id)
	s.bus.lock.Unlock()
}
