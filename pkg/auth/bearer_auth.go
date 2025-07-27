package auth

import (
	"fmt"
	"github.com/saturnines/nexus-core/pkg/errors"
	"net/http"
)

// BearerAuth implements the interface for Bearer token authentication
type BearerAuth struct {
	Token string // The bearer token
}

// NewBearerAuth creates a new bearer token authentication handler
func NewBearerAuth(token string) *BearerAuth {
	return &BearerAuth{
		Token: token,
	}
}

// ApplyAuth adds the Bearer token to the Authorization header
func (b *BearerAuth) ApplyAuth(req *http.Request) error {
	// Validate inputs
	if b.Token == "" {
		return errors.WrapError(
			fmt.Errorf("token is required"),
			errors.ErrConfiguration,
			"apply bearer auth",
		)
	}

	// Set the auth  header with the bearer token
	req.Header.Set("Authorization", "Bearer "+b.Token)

	return nil
}

// String returns a string representation of this auth method for testing
func (b *BearerAuth) String() string {
	// There is no need to actually put the actual token
	return "BearerAuth(token: [REDACTED])"
}
