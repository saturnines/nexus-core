package rest

import (
	"context"
	"net/http"

	"nexus-core/pkg/auth"
)

// Builder builds REST HTTP requests.
type Builder struct {
	URL         string
	Method      string
	Headers     map[string]string
	QueryParams map[string]string
	AuthHandler auth.Handler
}

// NewBuilder constructs a Builder.
// Method defaults to GET if empty.
func NewBuilder(
	url, method string,
	headers, params map[string]string,
	authHandler auth.Handler,
) *Builder {
	if method == "" {
		method = http.MethodGet
	}
	return &Builder{
		URL:         url,
		Method:      method,
		Headers:     headers,
		QueryParams: params,
		AuthHandler: authHandler,
	}
}

// Build creates an HTTP request.
func (b *Builder) Build(ctx context.Context) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, b.Method, b.URL, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range b.Headers {
		req.Header.Set(k, v)
	}

	if len(b.QueryParams) > 0 {
		q := req.URL.Query()
		for k, v := range b.QueryParams {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	if b.AuthHandler != nil {
		if err := b.AuthHandler.ApplyAuth(req); err != nil {
			return nil, err
		}
	}

	return req, nil
}
