package utils

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	defaultJitter = 0.15
	defaultMult   = 1.75
)

var (
	defaultBackoff = Backoff{
		MinDelay: 200 * time.Millisecond,
		MaxDelay: 5 * time.Second,
		Jitter:   defaultJitter,
		Mult:     defaultMult,
	}
)

// Backoff retry parameters
type Backoff struct {
	MinDelay time.Duration
	MaxDelay time.Duration
	Jitter   float64
	Mult     float64
}

// Retry executes the provided work function until success or until ctx is canceled.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryE(ctx context.Context, b *Backoff, work func() error) error {
	if b == nil {
		b = &defaultBackoff
	} else {
		if b.MinDelay > b.MaxDelay {
			b.MinDelay, b.MaxDelay = b.MaxDelay, b.MinDelay
		}

		if b.Jitter <= 0 {
			b.Jitter = defaultJitter
		}

		if b.Mult <= 0 {
			b.Mult = defaultMult
		}
	}

	mult := 1.0
	for ctx.Err() == nil {
		if err := work(); err == nil {
			return nil
		} else if err := ctx.Err(); err != nil {
			return err
		}

		delay := b.MinDelay * time.Duration(mult)
		if delay > b.MaxDelay {
			delay = b.MaxDelay
		} else {
			mult *= b.Mult
		}

		delay = AddJitter(delay, b.Jitter)

		select {
		case <-ctx.Done():
			return context.Canceled
		case <-time.After(delay):
		}
	}

	return ctx.Err()
}

// RetryB executes the provided work function until success or until ctx is canceled.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryB(ctx context.Context, b *Backoff, work func() error) bool {
	err := RetryE(ctx, b, work)
	return err == nil
}

// RetryR executes the provided work function until success or until ctx is canceled.
// Returns true and the result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryR[R any](ctx context.Context, b *Backoff, work func() (R, error)) (R, error) {
	var result R
	var err error

	work2 := func() error {
		result, err = work()
		return err
	}

	err = RetryE(ctx, b, work2)
	return result, err
}

// AddJitter adds random jitter in the range (-pct, +pct).
func AddJitter(d time.Duration, pct float64) time.Duration {
	if pct == 0 {
		return d
	}

	jitter := float64(d) * pct * (rand.Float64()*2 - 1)
	d += time.Duration(jitter)

	if d < 0 {
		return 0
	}
	return d
}
