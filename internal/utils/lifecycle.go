package utils

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

var (
	global atomic.Pointer[runInfo]
)

type runInfo struct {
	triggerShutdown func()
}

// Lifecycle defines methods for instance control.
type Lifecycle interface {
	Start(ctx context.Context) error
	Stop()
}

// LifecycleStart starts the provided instances.
func LifecycleStart[T Lifecycle](ctx context.Context, log logging.Logger, instances ...T) error {
	for i, instance := range instances {
		log.Tracef(ctx, "Starting %T...", instance)

		if err := instance.Start(ctx); err != nil {
			log.Debugf(ctx, "Start failed %T", instance)

			// rollback started instances so far
			LifecycleStop(log.NoContext(), instances[:i]...)

			return errors.Wrapf(err, "failed to start %T", instance)
		}

		log.Debugf(ctx, "Started %T", instance)
	}

	return nil
}

// LifecycleStop stops the provided instances.
func LifecycleStop[T Lifecycle](log logging.LoggerNoContext, instances ...T) {
	for i := len(instances) - 1; i >= 0; i-- {
		instance := instances[i]

		log.Tracef("Stopping %T...", instance)
		instance.Stop()
		log.Debugf("Stopped %T", instance)
	}
}

// LifecycleRun starts the instance and runs until the context is done.
func LifecycleRun[T Lifecycle](ctx context.Context, log logging.Logger, instance T) error {
	shutdownCh := make(chan any)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	global.Store(&runInfo{
		triggerShutdown: sync.OnceFunc(func() { close(shutdownCh) }),
	})

	// TODO: should be able to cancel start ctx to abort startup
	if err := LifecycleStart(ctx, log, instance); err != nil {
		return err
	}

	log.Info(ctx, "Running...")

	select {
	case <-signalCh:
		log.Debug(ctx, "Received interrupt signal.")
	case <-ctx.Done():
		log.Debug(ctx, "Context canceled.")
	case <-shutdownCh:
		log.Debug(ctx, "Shutdown was requested.")
	}

	log.Info(ctx, "Stopping...")

	LifecycleStop(log.NoContext(), instance)
	return nil
}

// GracefulShutdown allows shutting down the process in a graceful and controlled fashion.
func GracefulShutdown(message string) {
	info := global.Load()
	if info == nil {
		panic("GracefulShutdown: global run info not found")
	}

	logging.NoContext().Error("GracefulShutdown: %s", message)
	info.triggerShutdown()
}

// ShutdownNow triggers process shutdown and never returns.
func ShutdownNow(message string) {
	info := global.Load()
	if info != nil {
		info.triggerShutdown()
		time.Sleep(5 * time.Second)
	}

	logging.NoContext().Error("ShutdownNow: %s", message)
	panic(message)
}
