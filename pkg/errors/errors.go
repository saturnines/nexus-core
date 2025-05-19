// pkg/errors/errors.go

package errors

import (
	"errors"
	"fmt"
)

// Standard error types
var (
	ErrAuthentication = errors.New("authentication error")
	ErrConfiguration  = errors.New("configuration error")
	ErrHTTPRequest    = errors.New("HTTP request error")
	ErrHTTPResponse   = errors.New("HTTP response error")
	ErrPagination     = errors.New("pagination error")
	ErrExtraction     = errors.New("data extraction error")
	ErrTokenExpired   = errors.New("token expired")
	ErrValidation     = errors.New("validation error")
)

// WrapError wraps an error with a standard error type and a message
func WrapError(err error, errType error, message string) error {
	wrapped := fmt.Errorf("%s: %w", message, err)
	return fmt.Errorf("%w: %v", errType, wrapped)
}

// Is checks if an error is or contains the target error
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// Unwrap returns the wrapped error
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// As finds the first error in errors chain that matches target, and if so, sets
// target to that error value and returns true. Otherwise, it returns false.
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}
