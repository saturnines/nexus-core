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

// WrapError wraps an error with a standard error type
func WrapError(err error, errType error, message string) error {
	wrapped := fmt.Errorf("%s: %w", message, err)
	return fmt.Errorf("%w: %v", errType, wrapped)
}

// Is provides a convenience wrapper around errors.Is
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// Unwrap provides a convenience wrapper around errors.Unwrap
func Unwrap(err error) error {
	return errors.Unwrap(err)
}
