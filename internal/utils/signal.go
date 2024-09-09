package utils

import (
	"context"
	"os"
	"os/signal"

	"github.com/bcrusu/graph/internal/logging"
)

// WithCancelOnSignal returns a context that is canceled when the interrupt signal is received.
func WithCancelOnSignal(parent context.Context) context.Context {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	ctx, cancelFn := context.WithCancel(parent)

	go func() {
		<-signalCh
		logging.Debug(ctx, "Received interrupt signal.")
		cancelFn()
	}()

	// TODO: signal.NotifyContext()

	return ctx
}
