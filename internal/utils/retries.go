package utils

import (
	"context"
	"math"
	"time"

	"github.com/bcrusu/scout/internal/errors"
)

var (
	defaultBackoff = MustSetDefaults(&Backoff{})
)

type RetryPolicy struct {
	MaxAttempts int     `yaml:"maxAttempts" default:"3" validate:"min:1"`
	Backoff     Backoff `yaml:"backoff"`
}

// Backoff retry parameters
type Backoff struct {
	MinDelay   time.Duration `yaml:"minDelay" default:"100ms" validate:"min:10ms"`
	MaxDelay   time.Duration `yaml:"maxDelay" default:"2s" validate:"min:100ms"`
	Jitter     float64       `yaml:"jitter" default:"0.15" validate:"min:0,max:1"`
	Multiplier float64       `yaml:"multiplier" default:"1.75" validate:"min:1"`
}

// RetryE retries the provided work func.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryE(policy RetryPolicy, work func() error) error {
	return RetryContextE(context.Background(), policy, work)
}

// RetryB retries the provided work func.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryB(policy RetryPolicy, work func() error) bool {
	return RetryContextB(context.Background(), policy, work)
}

// RetryR retries the provided work func.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryR[R any](policy RetryPolicy, work func() (R, error)) (R, error) {
	return RetryContextR(context.Background(), policy, work)
}

// RetryContextE executes the provided work func for a max number of retries or until ctx is canceled.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextE(ctx context.Context, policy RetryPolicy, work func() error) error {
	SetDefaults(&policy)

	if policy.Backoff.MinDelay > policy.Backoff.MaxDelay {
		policy.Backoff.MinDelay, policy.Backoff.MaxDelay = policy.Backoff.MaxDelay, policy.Backoff.MinDelay
	}

	if policy.Backoff.Jitter <= 0 {
		policy.Backoff.Jitter = defaultBackoff.Jitter
	}

	if policy.Backoff.Multiplier <= 0 {
		policy.Backoff.Multiplier = defaultBackoff.Multiplier
	}

	var lastErr error
	retriesLeft := policy.MaxAttempts
	mult := 1.0

LOOP:
	for ctx.Err() == nil && retriesLeft > 0 {
		if lastErr = work(); lastErr == nil {
			return nil
		} else if ctx.Err() != nil {
			return lastErr
		}

		delay := policy.Backoff.MinDelay * time.Duration(mult)
		if delay > policy.Backoff.MaxDelay {
			delay = policy.Backoff.MaxDelay
		} else {
			mult *= policy.Backoff.Multiplier
		}

		delay = AddJitter(delay, policy.Backoff.Jitter)

		select {
		case <-ctx.Done():
			break LOOP
		case <-time.After(delay):
		}

		retriesLeft--
	}

	if lastErr != nil {
		return lastErr
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return errors.Errorf("retry failed %d times", policy.MaxAttempts)
}

// RetryContextB executes the provided work func for a max number of retries or until ctx is canceled.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextB(ctx context.Context, policy RetryPolicy, work func() error) bool {
	err := RetryContextE(ctx, policy, work)
	return err == nil
}

// RetryContextR executes the provided work func for a max number of retries or until ctx is canceled.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryContextR[R any](ctx context.Context, policy RetryPolicy, work func() (R, error)) (R, error) {
	var result R
	var err error

	work2 := func() error {
		result, err = work()
		return err
	}

	err = RetryContextE(ctx, policy, work2)
	return result, err
}

// RetryForeverE executes the provided work func until success or until ctx is canceled.
// Returns nil if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverE(ctx context.Context, backoff *Backoff, work func() error) error {
	b := backoff
	if b == nil {
		b = defaultBackoff
	}

	policy := RetryPolicy{MaxAttempts: math.MaxInt, Backoff: *b}
	return RetryContextE(ctx, policy, work)
}

// RetryForeverB executes the provided work func until success or until ctx is canceled.
// Returns true if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverB(ctx context.Context, b *Backoff, work func() error) bool {
	err := RetryForeverE(ctx, b, work)
	return err == nil
}

// RetryForeverR executes the provided work func until success or until ctx is canceled.
// Returns the work result if the function returned with success.
// If backoff is not provided will use the default values.
func RetryForeverR[R any](ctx context.Context, backoff *Backoff, work func() (R, error)) (R, error) {
	b := backoff
	if b == nil {
		b = defaultBackoff
	}

	policy := RetryPolicy{MaxAttempts: math.MaxInt, Backoff: *b}
	return RetryContextR(ctx, policy, work)
}
