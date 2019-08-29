package deqerr

import (
	"context"
	"errors"
	"fmt"
)

// Code is an error code used with errors created by this package.
type Code int

const (
	// Unknown indicates the cause of an error does not have designated meaning, including that an
	// error was not created with this package.
	Unknown Code = iota
	// Dup indicates an event is a duplicate of an existing event. Duplicates may be different from
	// the original event, but have the same ID.
	Dup
	// Unavailable indicates communication with a remote service failed.
	Unavailable
	// Internal indicates an internal error occurred.
	Internal
	// Invalid indicates the arguments of a request or call were invalid.
	Invalid
	// NotFound indicates the requested event was not found in the database.
	NotFound
	// Canceled indicates the request was canceled before it completed.
	Canceled
)

// String returns the string representation of c.
func (c Code) String() string {
	switch c {
	case Dup:
		return "Dup"
	case Unavailable:
		return "Unavailable"
	case Internal:
		return "Internal"
	case Invalid:
		return "Invalid"
	case NotFound:
		return "NotFound"
	case Canceled:
		return "Canceled"
	default:
		return "Unknown"
	}
}

// In order to benefit from fmt.Errorf's error wrapping functionality using %w, we use wrapError
// as an extra layer so deqError.Unwrap() can treat its Err as internal. For example, calling
// Errorf(Invalid, "example: %w", originalErr).Unwrap() should return originalErr even though it
// stores a new error internally, but
// WithMsgf(originalErr, "example2: %w", otherErr).Unwrap() should return originalErr, not otherErr.
type wrapError struct {
	Msg string
	Err error
}

func (err wrapError) Error() string {
	return err.Msg
}

func (err wrapError) Unwrap() error {
	return err.Err
}

type deqError struct {
	Err  error
	Code Code
}

// New returns a new error with the given code and message.
func New(code Code, msg string) error {
	return deqError{
		Err:  errors.New(msg),
		Code: code,
	}
}

// Errorf returns an error with the given code and a message formatted with fmt.Errorf.
func Errorf(code Code, format string, a ...interface{}) error {
	return deqError{
		Err:  fmt.Errorf(format, a...),
		Code: code,
	}
}

// Wrap wraps an error with an error that includes code, or nil if err is nil.
func Wrap(code Code, err error) error {
	if err == nil {
		return nil
	}

	return deqError{
		Err: wrapError{
			Err: err,
			Msg: err.Error(),
		},
		Code: code,
	}
}

// FromContext returns an error that wraps a context's error, or nil if the context has no error.
func FromContext(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	return Wrap(Canceled, err)
}

// Error returns the string representation of the error, including its code and message.
func (err deqError) Error() string {
	return fmt.Sprintf("deq error: code = %s desc = %s", err.Code, err.Err.Error())
}

// Unwrap returns the error that this error wraps, or nil if it doesn't wrap any error.
func (err deqError) Unwrap() error {
	u, ok := err.Err.(interface {
		Unwrap() error
	})
	if ok {
		return u.Unwrap()
	}
	return nil
}

// GetCode returns err's code if it was created by this package, or Unknown otherwise.
func GetCode(err error) Code {
	// TODO: support traversing an error chain with go 1.13 error wrapping.
	deqErr, ok := err.(deqError)
	if !ok {
		return Unknown
	}
	return deqErr.Code
}

// GetMsg returns err's message only, without including its code in the string.
func GetMsg(err error) string {
	deqErr, ok := err.(deqError)
	if !ok {
		return err.Error()
	}
	return deqErr.Err.Error()
}

// Unwrap is equivalent to errors.Unwrap(), and is copied here for clients not using go >= 1.13.
// For clients targeting go >= 1.13, it is reccommended to use errors.Unwrap() instead, as this
// function will eventually be removed.
func Unwrap(err error) error {
	u, ok := err.(interface {
		Unwrap() error
	})
	if !ok {
		return nil
	}
	return u.Unwrap()
}
