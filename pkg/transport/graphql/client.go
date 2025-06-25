package graphql

import "net/http"

// HTTPDoer is the same minimal interface used by rest.RequestHelper :contentReference[oaicite:1]{index=1}.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client executes GraphQL operations.
type Client struct {
	doer HTTPDoer
}

// NewClient wraps an HTTPDoer (e.g. *http.Client or a retry transport).
func NewClient(doer HTTPDoer) *Client {
	return &Client{doer: doer}
}

// Execute sends a built request.
func (c *Client) Execute(req *http.Request) (*http.Response, error) {
	return c.doer.Do(req)
}
