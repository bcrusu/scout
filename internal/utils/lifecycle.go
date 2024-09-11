package utils

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
)

const (
	ctxKeyShutdown contextKey = iota
)

type contextKey int

// Lifecycle defines methods for instance control.
type Lifecycle interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

// LifecycleStart starts the provided instances.
func LifecycleStart[T Lifecycle](ctx context.Context, log logging.Logger, instances ...T) error {
	for i, instance := range instances {
		log.Tracef(ctx, "Starting %T...", instance)

		if err := instance.Start(ctx); err != nil {
			log.Debugf(ctx, "Start failed %T", instance)

			// rollback started instances so far
			LifecycleStop(ctx, log, instances[:i]...)

			return errors.Wrapf(err, "failed to start %T", instance)
		}

		log.Debugf(ctx, "Started %T", instance)
	}

	return nil
}

// LifecycleStop stops the provided instances.
func LifecycleStop[T Lifecycle](ctx context.Context, log logging.Logger, instances ...T) {
	for i := len(instances) - 1; i >= 0; i-- {
		instance := instances[i]

		log.Tracef(ctx, "Stopping %T...", instance)
		instance.Stop(ctx)
		log.Debugf(ctx, "Stopped %T", instance)
	}
}

// LifecycleRun starts the instance and runs until the context is done.
func LifecycleRun[T Lifecycle](ctx context.Context, log logging.Logger, shutdownTimeout time.Duration, instance T) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	shutdownCh := make(chan any)
	ctx = context.WithValue(ctx, ctxKeyShutdown, shutdownCh)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	if err := LifecycleStart(ctx, log, instance); err != nil {
		return err
	}

	log.Info(ctx, "Running...")

	select {
	case <-signalCh:
		logging.Debug(ctx, "Received interrupt signal.")
	case <-ctx.Done():
		logging.Debug(ctx, "Context canceled.")
	case <-shutdownCh:
		logging.Debug(ctx, "Shutdown was requested.")
	}

	log.Info(ctx, "Stopping...")

	stopped := make(chan any)
	go func() {
		LifecycleStop(ctx, log, instance)
		close(stopped)
	}()

	select {
	case <-time.After(shutdownTimeout):
		log.Warn(ctx, "Failed to stop in the configured shutdown timeout. Canceling the context...")
		cancelCtx()
		<-time.After(time.Second)
	case <-stopped:
	}

	return nil
}

// LifecycleShutdown allows shutting down the process in a graceful and controlled fashion.
func LifecycleShutdown(ctx context.Context) {
	v := ctx.Value(ctxKeyShutdown)
	if v == nil {
		logging.Warn(ctx, "Context does not allow shutdown.")
	} else {
		close(v.(chan any))
	}
}
