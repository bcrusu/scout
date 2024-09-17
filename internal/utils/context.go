package utils

import (
	"context"
)

// WithCancelAndWaitR will cancel the work function and wait for it to complete.
func WithCancelAndWaitR[R any](work func(context.Context) R) (func(context.Context) R, context.CancelFunc) {
	signalCh := make(chan any, 1)
	var cancelFn context.CancelFunc

	workAndSignal := func(ctx context.Context) R {
		ctx, cancelFn = context.WithCancel(ctx)
		signalCh <- 1
		r := work(ctx)
		signalCh <- 1
		return r
	}

	cancelAndWait := func() {
		<-signalCh
		cancelFn()
		<-signalCh
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
	signalCh := make(chan any, 1)
	var cancelFn context.CancelFunc

	work2 := func(ctx context.Context) {
		ctx, cancelFn = context.WithCancel(ctx)
		signalCh <- 1
		work(ctx)
	}

	cancel := func() {
		<-signalCh
		cancelFn()
	}

	return work2, cancel
}
