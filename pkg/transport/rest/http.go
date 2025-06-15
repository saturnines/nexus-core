package rest

import (
	"bytes"
	"context"
	"io"
	"net/http"
)

// HTTPDoer is a minimal interface for HTTP clients
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// RequestHelper handles common HTTP request creation
func RequestHelper(
	ctx context.Context,
	doer HTTPDoer,
	method string,
	baseURL string,
	endpoint string,
	headers map[string]string,
	body []byte,
) (*http.Response, error) {
	url := baseURL
	if endpoint != "" {
		url = baseURL + endpoint
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	// Add headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// If body is present, assume JSON content type
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return doer.Do(req)
}
