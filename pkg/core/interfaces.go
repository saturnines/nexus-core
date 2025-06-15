package core

import (
	"context"
	"net/http"
)

// RequestBuilder builds the next request
type RequestBuilder interface {
	Build(ctx context.Context) (*http.Request, error)
}

// Pager inspects a response and tells if there’s more
type Pager interface {
	Update(resp *http.Response) error
	HasNext() bool
}
