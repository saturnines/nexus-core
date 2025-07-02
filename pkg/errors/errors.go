// pkg/errors/errors.go

package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	ErrGraphQL        = errors.New("GraphQL error")
)

// GraphQLError represents a single GraphQL error
type GraphQLError struct {
	Message    string                 `json:"message"`
	Path       []interface{}          `json:"path,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
	Locations  []ErrorLocation        `json:"locations,omitempty"`
}

// ErrorLocation represents the location of an error in the query
type ErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// GraphQLResponse represents the standard GraphQL response format
type GraphQLResponse struct {
	Data   interface{}    `json:"data"`
	Errors []GraphQLError `json:"errors,omitempty"`
}

// CheckGraphQLErrors examines response body for GraphQL errors
func CheckGraphQLErrors(body []byte) error {
	var response GraphQLResponse
	if err := json.Unmarshal(body, &response); err != nil {
		// If we can't parse as JSON, it's not a GraphQL response
		return nil
	}

	if len(response.Errors) == 0 {
		return nil
	}

	// Build error message from all GraphQL errors
	var messages []string
	for _, gqlErr := range response.Errors {
		msg := gqlErr.Message
		if len(gqlErr.Path) > 0 {
			msg = fmt.Sprintf("%s (path: %v)", msg, gqlErr.Path)
		}
		messages = append(messages, msg)
	}

	return WrapError(
		fmt.Errorf("GraphQL errors: %s", strings.Join(messages, "; ")),
		ErrGraphQL,
		"GraphQL response contained errors",
	)
}

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
