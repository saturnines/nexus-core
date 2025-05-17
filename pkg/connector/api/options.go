// pkg/connector/api/options.go

package api

import (
	"net/http"
	"time"
)

// HTTPClientOption configures an HTTPDoer
type HTTPClientOption func(HTTPDoer) HTTPDoer

// ApplyHTTPClientOptions applies multiple options to an HTTPDoer
func ApplyHTTPClientOptions(doer HTTPDoer, options ...HTTPClientOption) HTTPDoer {
	for _, option := range options {
		doer = option(doer)
	}
	return doer
}

// WithTimeout creates an option to set timeout on an HTTP client
func WithTimeout(timeout time.Duration) HTTPClientOption {
	return func(doer HTTPDoer) HTTPDoer {
		if httpClient, ok := doer.(*http.Client); ok {
			httpClient.Timeout = timeout
		}
		return doer
	}
}

// WithCustomHTTPClient creates an option that replaces the HTTPDoer entirely
func WithCustomHTTPClient(client HTTPDoer) HTTPClientOption {
	return func(_ HTTPDoer) HTTPDoer {
		return client
	}
}
