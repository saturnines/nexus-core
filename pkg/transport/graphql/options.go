package graphql

import (
	"github.com/saturnines/nexus-core/pkg/auth"
	"net/http"
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

// ApplyBuilderOptions applies opts in order.
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

// WithRetryPolicy wraps the Client’s Doer in a retry layer.
func WithRetryPolicy(policy RetryPolicy) ClientOption {
	return func(c *Client) {
		c.doer = NewRetryDoer(c.doer, policy)
	}
}

// ApplyClientOptions applies opts in order.
func (c *Client) ApplyOptions(opts ...ClientOption) {
	for _, opt := range opts {
		opt(c)
	}
}
