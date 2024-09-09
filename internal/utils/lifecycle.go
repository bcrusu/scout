package utils

import (
	"context"

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
	if err := LifecycleStart(ctx, log, instance); err != nil {
		return err
	}

	log.Info(ctx, "Running...")

	<-ctx.Done()
	log.Info(ctx, "Stopping...")

	LifecycleStop(ctx, log, instance)
	return nil
}
