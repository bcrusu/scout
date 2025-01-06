package agent

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
)

type Nemesis interface {
	Run(context.Context) error
}

type ctxKeyServiceName struct{}

func withServiceType(ctx context.Context, serviceType ServiceType) context.Context {
	return context.WithValue(ctx, ctxKeyServiceName{}, serviceType)
}

func getServiceType(ctx context.Context) (ServiceType, error) {
	v := ctx.Value(ctxKeyServiceName{})
	if v == nil {
		return 0, errors.NotFound
	}

	return v.(ServiceType), nil
}

// Kills the service using SIGKILL signal and systemd will auto restart it.
// Can be used to simulate hard crash-restart fault.
func (n *Kill) Run(ctx context.Context) error {
	serviceType, err := getServiceType(ctx)
	if err != nil {
		return err
	}

	return killService(serviceType, "SIGKILL")
}

// Pauses the service until ctx in done and then resumes it.
// Can be used to simulate a GC pause.
func (n *Pause) Run(ctx context.Context) error {
	serviceType, err := getServiceType(ctx)
	if err != nil {
		return err
	}

	if err := freezeService(serviceType); err != nil {
		return err
	}

	<-ctx.Done()
	return thawService(serviceType)
}

// Stops the service until ctx in done and then starts it back again.
// Can be used to simulate soft restart as in normal maintenance ops.
func (n *Restart) Run(ctx context.Context) error {
	serviceType, err := getServiceType(ctx)
	if err != nil {
		return err
	}

	if err := stopService(serviceType); err != nil {
		return err
	}

	<-ctx.Done()
	return startService(serviceType)
}

// BumpTime changes the current system time by the provided delta and
// then restores to original time when the ctx is done.
func (n *BumpTime) Run(ctx context.Context) error {
	delta := time.Duration(n.Delta) * time.Millisecond
	initialDiff := getTimeDiff()
	value := time.Now().Add(delta)
	if err := utils.SetTime(value); err != nil {
		return err
	}

	<-ctx.Done()
	return restoreTime(initialDiff)
}

// StrobeTime changes the current system time back and forth by adding
// then removing the provided delta, repeatedly, in each period.
func (n *StrobeTime) Run(ctx context.Context) error {
	initialDiff := getTimeDiff()
	count := 0
	delta := time.Duration(n.Delta) * time.Millisecond
	period := time.Duration(n.Period) * time.Millisecond

	for {
		var err error

		if count%2 == 0 {
			err = utils.SetTime(time.Now().Add(delta))
		} else {
			err = restoreTime(initialDiff)
		}

		if err != nil {
			log.WithError(err).Error("Failed to strobe time.")
		} else {
			count++
		}

		select {
		case <-time.After(period):
		case <-ctx.Done():
			return restoreTime(initialDiff)
		}
	}
}

func getTimeDiff() time.Duration {
	r := time.Now()
	m := utils.GetMonoTime()
	return r.Sub(m)
}

func restoreTime(initialDiff time.Duration) error {
	diff := initialDiff - getTimeDiff()
	if diff == 0 {
		return nil
	}

	value := time.Now().Add(diff)
	if err := utils.SetTime(value); err != nil {
		return errors.Wrap(err, "failed to restore time")
	}

	return nil
}
