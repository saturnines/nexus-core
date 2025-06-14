package auth

import (
	"encoding/base64"
	"fmt"
	"net/http"
)

// BasicAuth implements the interface for HTTP basic authentication
// Might be bad practice but I think yaml supports env variables
type BasicAuth struct {
	Username string // Username for Basic auth
	Password string // Password for Basic auth
}

// NewBasicAuth creates a new basic authentication handler
func NewBasicAuth(username, password string) *BasicAuth {
	return &BasicAuth{
		Username: username,
		Password: password,
	}
}

// ApplyAuth adds the basic auth header to the request
func (b *BasicAuth) ApplyAuth(req *http.Request) error {
	// Validate inputs
	if b.Username == "" {
		return fmt.Errorf("username is empty and is required for basic auth")
	}
	// don't think I need to validate pw bc it can be empty too

	// Create the "username:password" string
	authStr := b.Username + ":" + b.Password

	// encode the string
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(authStr))

	// Set the auth header
	req.Header.Set("Authorization", "Basic "+encodedAuth)

	return nil
}

// String returns a string representation of this auth method for testing
func (b *BasicAuth) String() string {
	return fmt.Sprintf("BasicAuth(username: %s)", b.Username)
}
