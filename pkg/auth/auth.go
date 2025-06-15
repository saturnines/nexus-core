package auth

import (
	"fmt"
	"net/http"
)

var (
	ErrInvalidCredentials = fmt.Errorf("invalid credentials")
	ErrTokenRefresh       = fmt.Errorf("token refresh failed")
	ErrMissingCredentials = fmt.Errorf("missing credentials")
)

// Handler defines the interface for auth handlers
type Handler interface {
	ApplyAuth(req *http.Request) error
}

// APIKeyAuth implements the Handler interface for API key authentication
type APIKeyAuth struct {
	HeaderName string // Header name for header-based auth (e.g., "X-API-Key")
	QueryParam string // Query parameter name for query-based auth (e.g., "api_key")
	Value      string // The actual API key value
}

// NewAPIKeyAuth creates a new API key authentication handler
// Either headerName OR queryParam should be provided (or both??)
func NewAPIKeyAuth(headerName, queryParam, value string) *APIKeyAuth {
	return &APIKeyAuth{
		HeaderName: headerName,
		QueryParam: queryParam,
		Value:      value,
	}
}

// ApplyAuth adds the API key to the request, either as a header or query parameter
func (a *APIKeyAuth) ApplyAuth(req *http.Request) error {
	// check that we have a value to use
	if a.Value == "" {
		return fmt.Errorf("API key value is required")
	}

	// If header name is added, add as a request header
	if a.HeaderName != "" {
		req.Header.Set(a.HeaderName, a.Value)
	}

	// If query parameter is added, add to the URL query string
	if a.QueryParam != "" {
		query := req.URL.Query()
		query.Set(a.QueryParam, a.Value)
		req.URL.RawQuery = query.Encode()
	}

	// If neither header nor query param was entered just return an error
	if a.HeaderName == "" && a.QueryParam == "" {
		return fmt.Errorf("API key auth requires either header name or query parameter name")
	}

	return nil
}

// String returns a string representation of this auth method
func (a *APIKeyAuth) String() string {
	if a.HeaderName != "" {
		return fmt.Sprintf("APIKeyAuth(header: %s)", a.HeaderName)
	}
	return fmt.Sprintf("APIKeyAuth(query: %s)", a.QueryParam)
}
