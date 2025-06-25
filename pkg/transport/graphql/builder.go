package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/saturnines/nexus-core/pkg/auth"
	"net/http"
)

// Builder constructs GraphQL requests.
type Builder struct {
	Endpoint    string
	Query       string
	Variables   map[string]interface{}
	Headers     map[string]string
	AuthHandler auth.Handler
}

// NewBuilder sets up a GraphQL Builder.
// Endpoint is the full URL of your GraphQL endpoint.
func NewBuilder(
	endpoint, query string,
	variables map[string]interface{},
	headers map[string]string,
	authHandler auth.Handler,
) *Builder {
	return &Builder{
		Endpoint:    endpoint,
		Query:       query,
		Variables:   variables,
		Headers:     headers,
		AuthHandler: authHandler,
	}
}

// Build creates the *http.Request with JSON body.
func (b *Builder) Build(ctx context.Context) (*http.Request, error) {
	body := map[string]interface{}{
		"query":     b.Query,
		"variables": b.Variables,
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Endpoint, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	for k, v := range b.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	if b.AuthHandler != nil {
		if err := b.AuthHandler.ApplyAuth(req); err != nil {
			return nil, err
		}
	}
	return req, nil
}
