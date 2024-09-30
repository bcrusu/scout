package utils

type Queue[T any] struct {
	head *queueNode[T]
	tail *queueNode[T]
}

type queueNode[T any] struct {
	value T
	next  *queueNode[T]
	prev  *queueNode[T]
}

func NewQueue[T any](items ...T) *Queue[T] {
	q := &Queue[T]{}
	for _, t := range items {
		q.PushBack(t)
	}
	return q
}

func (q *Queue[T]) IsEmpty() bool {
	return q.head == nil
}

func (q *Queue[T]) PushFront(t T) {
	node := &queueNode[T]{value: t}

	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		node.next = q.head
		q.head.prev = node
		q.head = node
	}
}

func (q *Queue[T]) PushBack(t T) {
	node := &queueNode[T]{value: t}

	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		node.prev = q.tail
		q.tail.next = node
		q.tail = node
	}
}

func (q *Queue[T]) PopFront() (t T, ok bool) {
	if q.IsEmpty() {
		return t, false
	}

	result := q.head.value

	if q.head.next == nil {
		q.head = nil
		q.tail = nil
	} else {
		q.head = q.head.next
		q.head.prev = nil
	}

	return result, true
}

func (q *Queue[T]) PopBack() (t T, ok bool) {
	if q.IsEmpty() {
		return t, false
	}

	result := q.tail.value

	if q.tail.prev == nil {
		q.head = nil
		q.tail = nil
	} else {
		q.tail = q.tail.prev
		q.tail.next = nil
	}

	return result, true
}

func (q *Queue[T]) PeekFront() (t T, ok bool) {
	if q.IsEmpty() {
		return t, false
	}

	return q.head.value, true
}

func (q *Queue[T]) PeekBack() (t T, ok bool) {
	if q.IsEmpty() {
		return t, false
	}

	return q.tail.value, true
}
