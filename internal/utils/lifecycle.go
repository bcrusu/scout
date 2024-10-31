package utils

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

var (
	logLifecycle = logging.New("lifecycle").NoContext()
	global       atomic.Pointer[runInfo]
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
			log.Tracef(ctx, "Start failed %T", instance)

			// rollback started instances so far
			LifecycleStop(log.NoContext(), instances[:i]...)

			return errors.Wrapf(err, "failed to start %T", instance)
		}

		log.Tracef(ctx, "Started %T", instance)
	}

	return nil
}

// LifecycleStop stops the provided instances.
func LifecycleStop[T Lifecycle](log logging.LoggerNoContext, instances ...T) {
	for i := len(instances) - 1; i >= 0; i-- {
		instance := instances[i]

		log.Tracef("Stopping %T...", instance)
		instance.Stop()
		log.Tracef("Stopped %T", instance)
	}
}

// LifecycleRun starts the instance and runs until a stop signal is received.
func LifecycleRun(ctx context.Context, log logging.Logger, instance Lifecycle) error {
	shutdownCh := make(chan any)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	global.Store(&runInfo{
		triggerShutdown: sync.OnceFunc(func() { close(shutdownCh) }),
	})

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	resultCh := make(chan error)
	cancelCh := make(chan any)

	go func() {
		resultCh <- instance.Start(cancelCtx)
	}()

	go func() {
		select {
		case <-signalCh:
			log.Debug(ctx, "Received interrupt signal.")
		case <-ctx.Done():
			log.Debug(ctx, "Context canceled.")
		case <-shutdownCh:
			log.Debug(ctx, "Shutdown was requested.")
		}

		close(cancelCh)
	}()

	select {
	case <-cancelCh:
		cancelFunc()
		return <-resultCh
	case err := <-resultCh:
		if err != nil {
			return err
		}
	}

	log.Info(ctx, "Running...")
	<-cancelCh
	log.Info(ctx, "Stopping...")

	instance.Stop()
	return nil
}

// GracefulShutdown allows shutting down the process in a graceful and controlled fashion.
func GracefulShutdown(message string) {
	info := global.Load()
	if info == nil {
		panic("GracefulShutdown: global run info not found")
	}

	logLifecycle.Error("GracefulShutdown: %s", message)
	info.triggerShutdown()
}

// ShutdownNow triggers process shutdown and never returns.
func ShutdownNow(message string) {
	info := global.Load()
	if info != nil {
		info.triggerShutdown()
		time.Sleep(5 * time.Second)
	}

	logLifecycle.Error("ShutdownNow: %s", message)
	panic(message)
}

// ShutdownNowf triggers process shutdown and never returns.
func ShutdownNowf(format string, args ...any) {
	ShutdownNow(fmt.Sprintf(format, args...))
}

func RunAsync(ctx context.Context, work func(ctx context.Context)) context.CancelFunc {
	signalCh := make(chan any)
	cancelCtx, cancelFunc := context.WithCancel(ctx)

	go func() {
		work(cancelCtx)
		close(signalCh)
	}()

	cancelAndWait := func() {
		cancelFunc()
		<-signalCh
	}

	return cancelAndWait
}
