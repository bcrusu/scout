package utils

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

// Drainer will cancel all in-flight work when Stop is called or when the context is canceled.
type Drainer struct {
	log      logging.Logger
	drainCh  chan any
	inFlight atomic.Int64
}

func NewDrainer(log logging.Logger) *Drainer {
	return &Drainer{
		log:     log,
		drainCh: make(chan any),
	}
}

func (d *Drainer) Stop() {
	close(d.drainCh)

	backoff := &Backoff{
		MinDelay: 5 * time.Millisecond,
		MaxDelay: 100 * time.Millisecond,
	}

	err := RetryForeverE(context.Background(), backoff, func() error {
		pending := d.inFlight.Load()
		if pending == 0 {
			return nil
		}
		return errors.Errorf("waiting for %d in-flight contexts.", pending)
	})

	if err != nil {
		d.log.WithError(err).Warn("Drain failed.")
	} else {
		d.log.Debug("Drain success.")
	}
}

// WithDrain returns a child context that will be canceled when drainer is stopped.
// If already draining, it will return a canceled context.
// For all scenarios, the caller must invoke the cancel func when work is done.
func (d *Drainer) WithDrain(ctx context.Context) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(ctx)

	select {
	case <-d.drainCh:
		cancel()
		return cctx, cancel
	default:
		d.inFlight.Add(1)
	}

	// Is there a better approach that avoids spawning a goroutine per request?
	// Keeping track of in-flight contexts using a dictionary is approx two times
	// slower than the current approach (benchmarked it).
	go func() {
		select {
		case <-d.drainCh:
			cancel()
		case <-cctx.Done():
		}
	}()

	return cctx, func() { cancel(); d.inFlight.Add(-1) }
}
