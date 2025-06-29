package graphql

import (
	"net/http"
	"time"

	"github.com/saturnines/nexus-core/pkg/auth"
)

// BuilderOption configures the Builder.
type BuilderOption func(*Builder)

// WithHeader adds a header to every GraphQL request.
func WithHeader(key, value string) BuilderOption {
	return func(b *Builder) {
		if b.Headers == nil {
			b.Headers = make(map[string]string)
		}
		b.Headers[key] = value
	}
}

// WithHeaders adds multiple headers to every GraphQL request.
func WithHeaders(headers map[string]string) BuilderOption {
	return func(b *Builder) {
		if b.Headers == nil {
			b.Headers = make(map[string]string)
		}
		for k, v := range headers {
			b.Headers[k] = v
		}
	}
}

// WithAuthHandler sets a custom auth handler.
func WithAuthHandler(h auth.Handler) BuilderOption {
	return func(b *Builder) {
		b.AuthHandler = h
	}
}

// WithEndpoint overrides the default endpoint.
func WithEndpoint(url string) BuilderOption {
	return func(b *Builder) {
		b.Endpoint = url
	}
}

// WithQuery overrides the default query.
func WithQuery(query string) BuilderOption {
	return func(b *Builder) {
		b.Query = query
	}
}

// WithVariable sets a single variable.
func WithVariable(key string, value interface{}) BuilderOption {
	return func(b *Builder) {
		if b.Variables == nil {
			b.Variables = make(map[string]interface{})
		}
		b.Variables[key] = value
	}
}

// WithVariables sets multiple variables.
func WithVariables(variables map[string]interface{}) BuilderOption {
	return func(b *Builder) {
		if b.Variables == nil {
			b.Variables = make(map[string]interface{})
		}
		for k, v := range variables {
			b.Variables[k] = v
		}
	}
}

// ApplyOptions applies BuilderOption functions in order.
func (b *Builder) ApplyOptions(opts ...BuilderOption) {
	for _, opt := range opts {
		opt(b)
	}
}

// ClientOption configures the Client.
type ClientOption func(*Client)

// WithHTTPDoer swaps the underlying HTTPDoer.
func WithHTTPDoer(doer HTTPDoer) ClientOption {
	return func(c *Client) {
		c.doer = doer
	}
}

// WithTimeout sets a timeout on the HTTP client (if it's an *http.Client).
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		if httpClient, ok := c.doer.(*http.Client); ok {
			httpClient.Timeout = timeout
		}
	}
}

// WithUserAgent sets the User Agent header for requests.
func WithUserAgent(userAgent string) ClientOption {
	return func(c *Client) {
		// This would require modifying Client to store headers
		// For now, it's better to use WithHeader on Builder
	}
}

// ApplyOptions applies ClientOption functions in order.
func (c *Client) ApplyOptions(opts ...ClientOption) {
	for _, opt := range opts {
		opt(c)
	}
}
