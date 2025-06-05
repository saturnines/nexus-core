package auth

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// OAuth2RoundTripper wraps an OAuth2Auth handler to provide automatic token refresh on 401
type OAuth2RoundTripper struct {
	base     http.RoundTripper
	oauth2   *OAuth2Auth
	retryMap map[string]bool // Track retries by request ID instead of pointer
	retryMux sync.Mutex
}

// NewOAuth2RoundTripper creates a new OAuth2RoundTripper
func NewOAuth2RoundTripper(base http.RoundTripper, oauth2Auth *OAuth2Auth) *OAuth2RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &OAuth2RoundTripper{
		base:     base,
		oauth2:   oauth2Auth,
		retryMap: make(map[string]bool),
	}
}

// RoundTrip implements http.RoundTripper
func (rt *OAuth2RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Create a unique request ID for retry tracking
	requestID := fmt.Sprintf("%s:%s:%p", req.Method, req.URL.String(), req)

	// Clone the request to avoid modifying the original
	reqCopy := rt.cloneRequest(req)

	// Apply OAuth2 token
	if err := rt.oauth2.ApplyAuth(reqCopy); err != nil {
		return nil, fmt.Errorf("failed to apply OAuth2 auth: %w", err)
	}

	// Send the request
	resp, err := rt.base.RoundTrip(reqCopy)
	if err != nil {
		return resp, err
	}

	// If not 401, return as-is
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	// Check if we've already retried this request
	rt.retryMux.Lock()
	alreadyRetried := rt.retryMap[requestID]
	if !alreadyRetried {
		rt.retryMap[requestID] = true
	}
	rt.retryMux.Unlock()

	if alreadyRetried {
		// Already retried once, don't retry again
		return resp, nil
	}

	// Clean up retry tracking after some time
	go func() {
		time.Sleep(5 * time.Minute)
		rt.retryMux.Lock()
		delete(rt.retryMap, requestID)
		rt.retryMux.Unlock()
	}()

	// Close the 401 response body
	resp.Body.Close()

	// Force token refresh by clearing current token
	rt.oauth2.mutex.Lock()
	rt.oauth2.accessToken = ""
	rt.oauth2.expiresAt = time.Now().Add(-time.Hour) // Force expiry
	rt.oauth2.mutex.Unlock()

	// Clone the request again for retry
	retryCopy := rt.cloneRequest(req)

	// Apply auth again (this should trigger refresh)
	if err := rt.oauth2.ApplyAuth(retryCopy); err != nil {
		return nil, err
	}

	// Retry the request
	return rt.base.RoundTrip(retryCopy)
}

// cloneRequest creates a copy of the request with a fresh body
func (rt *OAuth2RoundTripper) cloneRequest(req *http.Request) *http.Request {
	clone := req.Clone(req.Context())

	// Clone the body if present
	if req.Body != nil {
		bodyBytes, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))   // Restore original
		clone.Body = io.NopCloser(bytes.NewReader(bodyBytes)) // Clone gets copy
	}

	return clone
}
