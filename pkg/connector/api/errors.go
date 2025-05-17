package api

import (
	"errors"
	"fmt"
)

// standard error types
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

// WrapError wraps an error with types
func WrapError(err error, errType error, message string) error {
	return fmt.Errorf("%w: %s: %w", errType, message, err)
}
