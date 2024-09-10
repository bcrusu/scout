package utils

import (
	"context"
	"os"
	"os/signal"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
)

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
func LifecycleRun[T Lifecycle](ctx context.Context, log logging.Logger, instance T) error {
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
	}

	log.Info(ctx, "Stopping...")

	LifecycleStop(ctx, log, instance) // TODO: use diff ctx?
	return nil
}
