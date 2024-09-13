package utils

import (
	"sync"
	"sync/atomic"
)

type Publisher[T any] interface {
	Subscribe(bufferSize int) Subscriber[T]
	Publish(T)
	PublishAttempt(T) bool
	NotifyChan() <-chan any
}

type Subscriber[T any] interface {
	Item() T
	ItemChan() <-chan T
	Unsubscribe()
	NotifyPublisher()
}

type publisher[T any] struct {
	nextID   atomic.Uint64
	minSize  int
	subsLock sync.RWMutex
	subs     map[uint64]*subscriber[T]
	notifyCh chan any
}

type subscriber[T any] struct {
	id           uint64
	buffer       chan T
	unsubscribed atomic.Bool
	signalFn     func(subID uint64, unsubscribe bool)
}

func NewPubSub[T any](minSize int) Publisher[T] {
	return &publisher[T]{
		minSize:  minSize,
		subs:     map[uint64]*subscriber[T]{},
		notifyCh: make(chan any, 1),
	}
}

func (p *publisher[T]) Subscribe(bufferSize int) Subscriber[T] {
	if bufferSize < p.minSize {
		bufferSize = p.minSize
	}

	sub := &subscriber[T]{
		id:       p.nextID.Add(1),
		buffer:   make(chan T, bufferSize),
		signalFn: p.signal,
	}
	p.subsLock.Lock()
	p.subs[sub.id] = sub
	p.subsLock.Unlock()
	return sub
}

func (p *publisher[T]) Publish(t T) {
	p.subsLock.RLock()
	for _, sub := range p.subs {
		sub.buffer <- t
	}
	p.subsLock.RUnlock()
}

func (p *publisher[T]) PublishAttempt(t T) bool {
	p.subsLock.RLock()
	allSuccess := true
	for _, sub := range p.subs {
		select {
		case sub.buffer <- t:
		default:
			allSuccess = false
		}
	}
	p.subsLock.RUnlock()
	return allSuccess
}

func (s *publisher[T]) NotifyChan() <-chan any {
	return s.notifyCh
}

func (p *publisher[T]) signal(subID uint64, unsubscribe bool) {
	if unsubscribe {
		p.subsLock.Lock()
		delete(p.subs, subID)
		p.subsLock.Unlock()
	} else {
		if len(p.notifyCh) == 0 {
			p.notifyCh <- nil
		}
	}
}

func (s *subscriber[T]) Item() T {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	return <-s.buffer
}

func (s *subscriber[T]) ItemChan() <-chan T {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	return s.buffer
}

func (s *subscriber[T]) Unsubscribe() {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	s.signalFn(s.id, true)
	s.unsubscribed.Store(true)
}

func (s *subscriber[T]) NotifyPublisher() {
	if s.unsubscribed.Load() {
		panic("subscriber has unsubscribed")
	}
	s.signalFn(s.id, false)
}
