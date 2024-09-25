package utils

import (
	"context"
	"math"
	"time"

	"github.com/bcrusu/graph/internal/errors"
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

// RetryE executes the provided work func for a max number of retries.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryE(maxRetries int, b *Backoff, work func() error) error {
	return RetryContextE(context.Background(), maxRetries, b, work)
}

// RetryB executes the provided work func for a max number of retries.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryB(maxRetries int, b *Backoff, work func() error) bool {
	return RetryContextB(context.Background(), maxRetries, b, work)
}

// RetryR executes the provided work func for a max number of retries.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryR[R any](maxRetries int, b *Backoff, work func() (R, error)) (R, error) {
	return RetryContextR(context.Background(), maxRetries, b, work)
}

// RetryContextE executes the provided work func for a max number of retries or until ctx is canceled.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextE(ctx context.Context, maxRetries int, b *Backoff, work func() error) error {
	retriesLeft := maxRetries
	if retriesLeft < 1 {
		retriesLeft = 1
	}

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
	for ctx.Err() == nil && retriesLeft > 0 {
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

		retriesLeft--
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return errors.Errorf("retry failed %d times", maxRetries)
}

// RetryContextB executes the provided work func for a max number of retries or until ctx is canceled.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextB(ctx context.Context, maxRetries int, b *Backoff, work func() error) bool {
	err := RetryContextE(ctx, maxRetries, b, work)
	return err == nil
}

// RetryContextR executes the provided work func for a max number of retries or until ctx is canceled.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextR[R any](ctx context.Context, maxRetries int, b *Backoff, work func() (R, error)) (R, error) {
	var result R
	var err error

	work2 := func() error {
		result, err = work()
		return err
	}

	err = RetryContextE(ctx, maxRetries, b, work2)
	return result, err
}

// RetryForeverE executes the provided work func until success or until ctx is canceled.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverE(ctx context.Context, b *Backoff, work func() error) error {
	return RetryContextE(ctx, math.MaxInt, b, work)
}

// RetryForeverB executes the provided work func until success or until ctx is canceled.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverB(ctx context.Context, b *Backoff, work func() error) bool {
	return RetryContextB(ctx, math.MaxInt, b, work)
}

// RetryForeverR executes the provided work func until success or until ctx is canceled.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverR[R any](ctx context.Context, b *Backoff, work func() (R, error)) (R, error) {
	return RetryContextR(ctx, math.MaxInt, b, work)
}
