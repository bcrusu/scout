package utils

import (
	"context"
	"sync"
)

// WithCancelAndWaitR will cancel the work function and wait for it to complete.
func WithCancelAndWaitR[R any](work func(context.Context) R) (func(context.Context) R, context.CancelFunc) {
	doneCh := make(chan any)
	var wg sync.WaitGroup
	var cancelFn context.CancelFunc
	wg.Add(1) // TODO review

	workAndSignal := func(ctx context.Context) R {
		ctx, cancelFn = context.WithCancel(ctx)
		wg.Done()
		r := work(ctx)
		close(doneCh)
		return r
	}

	cancelAndWait := func() {
		wg.Wait()
		cancelFn()
		<-doneCh
	}

	return workAndSignal, cancelAndWait
}

// WithCancelAndWait will cancel the work function and wait for it to complete.
func WithCancelAndWait(work func(context.Context)) (func(context.Context), context.CancelFunc) {
	w1 := func(ctx context.Context) any { work(ctx); return nil }

	workAndSignal, cancelAndWait := WithCancelAndWaitR(w1)

	w2 := func(ctx context.Context) { workAndSignal(ctx) }

	return w2, cancelAndWait
}

// WithCancel will cancel the work function and does not for it to complete.
func WithCancel(work func(context.Context)) (func(context.Context), context.CancelFunc) {
	var wg sync.WaitGroup
	var cancelFn context.CancelFunc
	wg.Add(1)

	work2 := func(ctx context.Context) {
		ctx, cancelFn = context.WithCancel(ctx)
		wg.Done()
		work(ctx)
	}

	cancel := func() {
		wg.Wait()
		cancelFn()
	}

	return work2, cancel
}
