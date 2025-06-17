package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/saturnines/nexus-core/pkg/config"
)

// HTTPError wraps HTTP error responses
type HTTPError struct {
	StatusCode int
	Status     string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Status)
}

type RetryTransport struct {
	Base   http.RoundTripper
	Cfg    *config.RetryConfig
	jitter *rand.Rand // Localized jitter source
}

// NewRetryTransport creates a new retry transport
func NewRetryTransport(base http.RoundTripper, cfg *config.RetryConfig) *RetryTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &RetryTransport{
		Base:   base,
		Cfg:    cfg,
		jitter: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (t *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Cfg == nil || t.Cfg.MaxAttempts <= 1 {
		return t.Base.RoundTrip(req)
	}

	switch req.Method {
	case http.MethodGet, http.MethodHead,
		http.MethodPut, http.MethodDelete,
		http.MethodOptions, http.MethodTrace:
		// retryable for idempotency methods
	default:
		return t.Base.RoundTrip(req)
	}

	var lastErr error
	var lastResp *http.Response

	for attempt := 0; attempt < t.Cfg.MaxAttempts; attempt++ {
		// Clone request for safe keeping
		// A lazy way in theory could be just clone if body of http request is "small"
		req2 := t.cloneRequest(req)

		resp, err := t.Base.RoundTrip(req2)

		// Network error handling with proper interface detection
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && (netErr.Temporary() || netErr.Timeout()) {
				// retryable network error
				lastErr = err
			} else {
				// Non retryable network error
				return nil, err
			}
		} else {
			// Success
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if lastResp != nil {
					lastResp.Body.Close()
				}
				return resp, nil
			}

			// Check if status code is configured as non retryable
			if !t.contains(t.Cfg.RetryableStatuses, resp.StatusCode) {
				// Close any previous response
				if lastResp != nil {
					lastResp.Body.Close()
				}
				// Return HTTP error for 4xx/5xx that aren't retryable, this should cover 99% of cases
				if resp.StatusCode >= 400 {
					return nil, &HTTPError{
						StatusCode: resp.StatusCode,
						Status:     resp.Status,
					}
				}
				return resp, nil
			}

			// Store for retry
			if lastResp != nil {
				lastResp.Body.Close()
			}
			lastResp = resp
		}

		// Check if context was cancelled
		if ctxErr := req.Context().Err(); ctxErr != nil {
			if lastResp != nil {
				lastResp.Body.Close()
			}
			return nil, ctxErr
		}

		// Don't wait after the last attempt
		if attempt < t.Cfg.MaxAttempts-1 {
			delay := t.backoff(attempt)

			select {
			case <-req.Context().Done():
				if lastResp != nil {
					lastResp.Body.Close()
				}
				return nil, req.Context().Err()
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	// All retries exhausted
	if lastResp != nil {
		// Return the last response even if it was an error status
		return lastResp, nil
	}

	// Return the last error if no response was received
	if lastErr != nil {
		return nil, fmt.Errorf("retry transport failed after %d attempts: %w", t.Cfg.MaxAttempts, lastErr)
	}
	return nil, fmt.Errorf("retry transport failed after %d attempts: no response received", t.Cfg.MaxAttempts)
}

// cloneRequest makes a deep copy for safe body reuse
func (t *RetryTransport) cloneRequest(r *http.Request) *http.Request {
	r2 := r.Clone(r.Context())
	if r.Body != nil {
		buf, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(buf))
		r2.Body = io.NopCloser(bytes.NewReader(buf))
	}
	return r2
}

// backoff computes full jitter exponential backoff
func (t *RetryTransport) backoff(attempt int) time.Duration {
	// Convert seconds to time.Duration
	base := time.Duration(t.Cfg.InitialBackoff * float64(time.Second))

	// Calculate max delay for this attempt
	maxDelay := time.Duration(float64(base) * math.Pow(t.Cfg.BackoffMultiplier, float64(attempt)))

	// Cap at 30 seconds
	if maxDelay > 30*time.Second {
		maxDelay = 30 * time.Second
	}

	// Full jitter: random duration between 0 and max
	return time.Duration(t.jitter.Float64() * float64(maxDelay))
}

// contains checks if slice contains value
func (t *RetryTransport) contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
