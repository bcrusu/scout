package errors

import (
	"errors"
	"fmt"
)

// Wrap wraps the error with a message
func Wrap(err error, message string) error {
	return fmt.Errorf("%s error=%w", message, err)
}

// Wrapf wraps the error with a formatted message
func Wrapf(err error, format string, args ...any) error {
	return fmt.Errorf("%s error=%w", fmt.Sprintf(format, args...), err)
}

// Error returns a new error
func Error(message string) error {
	return errors.New(message)
}

// Errorf returns a new error with formatted message
func Errorf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

// Join wraps the provided errors. Returns nil if all are nil.
func Join(errs ...error) error {
	return errors.Join(errs...)
}

// Is returns true if err matches the other err.
func Is(err, other error) bool {
	return errors.Is(err, other)
}

// Is returns true if err matches any other error.
func IsAny(err error, others ...error) bool {
	for _, other := range others {
		if Is(err, other) {
			return true
		}
	}
	return false
}

// Assert stops the show right quick when err != nil.
func Assert(err error) {
	if err != nil {
		panic(Wrap(err, "assert failed."))
	}
}
