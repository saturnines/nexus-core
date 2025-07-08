package rest_e2e_tests_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/saturnines/nexus-core/pkg/config"

	errors2 "github.com/saturnines/nexus-core/pkg/errors"
)

// TEST 1: Rate limit on first request
func TestConnector_RateLimit_429Response(t *testing.T) {
	tests := []struct {
		name          string
		retryAfter    string // Retry After header value
		whenToFail    int    // request number should return 429
		expectError   bool
		errorContains string
		description   string
	}{
		{
			name:          "Rate limit on first request",
			retryAfter:    "60",
			whenToFail:    1,
			expectError:   true,
			errorContains: "429",
			description:   "Rate limited immediately - should surface error",
		},
		{
			name:          "Rate limit during pagination",
			retryAfter:    "30",
			whenToFail:    2,
			expectError:   true,
			errorContains: "429",
			description:   "Rate limited on second page - should surface error",
		},
		{
			name:          "Rate limit with date format",
			retryAfter:    "Wed, 21 Oct 2015 07:28:00 GMT",
			whenToFail:    1,
			expectError:   true,
			errorContains: "429",
			description:   "Rate limit with HTTP date format",
		},
		{
			name:          "Rate limit without retry-after",
			retryAfter:    "", // No Retry After header
			whenToFail:    1,
			expectError:   true,
			errorContains: "429",
			description:   "Rate limit without retry guidance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := 0

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++

				// Return 429 on specified request
				if requestCount == tt.whenToFail {
					w.WriteHeader(http.StatusTooManyRequests)
					if tt.retryAfter != "" {
						w.Header().Set("Retry-After", tt.retryAfter)
					}
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte(`{
						"error": "rate_limit_exceeded",
						"message": "API rate limit exceeded. Please try again later."
					}`))
					return
				}

				// Normal successful response
				page := requestCount
				response := map[string]interface{}{
					"items": []interface{}{
						map[string]interface{}{
							"id":   page,
							"name": fmt.Sprintf("Item %d", page),
						},
					},
					"has_more": requestCount < 3, // More pages available
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "rate-limit-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						Fields: []config.Field{
							{Name: "id", Path: "id"},
							{Name: "name", Path: "name"},
						},
					},
				},
				Pagination: &config.Pagination{
					Type:        config.PaginationTypePage,
					PageParam:   "page",
					SizeParam:   "size",
					PageSize:    1,
					HasMorePath: "has_more",
				},
			}

			connector, err := core.NewConnector(cfg)
			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}

			ctx := context.Background()
			results, err := connector.Extract(ctx)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.description)
					return
				}

				// Verify error type and message
				// Rate limits during pagination are caught by the pager, so expect ErrPagination
				if !errors2.Is(err, errors2.ErrPagination) {
					t.Errorf("Expected ErrPagination, got error type: %T", err)
				}

				if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got: %s", tt.errorContains, err.Error())
				}

				t.Logf("%s: correctly returned error: %v", tt.description, err)
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tt.description, err)
					return
				}

				t.Logf("%s: successfully handled, got %d results", tt.description, len(results))
			}

			// Verify request count makes sense
			expectedMaxRequests := tt.whenToFail
			if requestCount > expectedMaxRequests {
				t.Errorf("Expected at most %d requests, got %d", expectedMaxRequests, requestCount)
			}

			t.Logf("Made %d requests before rate limit/completion", requestCount)
		})
	}
}

// TEST 2: Rate limiting on single (non-paginated) requests
func TestConnector_RateLimit_SingleRequest(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always return 429 for single request scenario
		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Retry-After", "60")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"error": "rate_limit_exceeded",
			"message": "Rate limit exceeded. Please try again in 60 seconds."
		}`))
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "single-request-rate-limit-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
		},
		// No pagination, single request
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected rate limit error, got nil")
	}

	// For single requests, rate limits should be ErrHTTPResponse
	if !errors2.Is(err, errors2.ErrHTTPResponse) {
		t.Errorf("Expected ErrHTTPResponse for single request, got error type: %T", err)
	}

	if !strings.Contains(err.Error(), "429") {
		t.Errorf("Error should mention status 429, got: %s", err.Error())
	}

	t.Logf("Successfully handled rate limit on single request: %v", err)
}

// TEST 3: Rate limiting with Link header pagination
func TestConnector_RateLimit_LinkPagination(t *testing.T) {
	requestCount := 0
	baseURL := ""

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if baseURL == "" {
			baseURL = "http://" + r.Host
		}
		requestCount++

		// Rate limit on second request
		if requestCount == 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Header().Set("Retry-After", "120")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"error": "rate_limit_exceeded",
				"message": "Too many requests. Retry after 120 seconds.",
				"retry_after_seconds": 120
			}`))
			return
		}

		// First request succeeds with Link header
		if requestCount == 1 {
			nextURL := fmt.Sprintf("%s/page2", baseURL)
			w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next"`, nextURL))
		}

		response := map[string]interface{}{
			"data": []interface{}{
				map[string]interface{}{
					"id":   requestCount,
					"name": fmt.Sprintf("Link Item %d", requestCount),
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "rate-limit-link-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected rate limit error, got nil")
	}

	// Should have made exactly 2 requests (success + rate limited)
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	// Verify error mentions rate limiting (status 429)
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("Error should mention status 429, got: %s", err.Error())
	}

	t.Logf("Successfully detected rate limit during Link pagination: %v", err)
}

// TEST 4: Rate limiting with OAuth2 authentication
func TestConnector_RateLimit_OAuth2_WithAuth(t *testing.T) {
	// OAuth2 mock server (should succeed)
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"access_token": "test_token_123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	// API mock server (rate limits after OAuth2 auth)
	requestCount := 0
	apiMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Check for auth header
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "missing_authorization"}`))
			return
		}

		// Rate limit authenticated request
		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Retry-After", "300")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10))
		w.Write([]byte(`{
			"error": "rate_limit_exceeded", 
			"message": "API rate limit exceeded for authenticated user",
			"retry_after": 300
		}`))
	}))
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "rate-limit-oauth2-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected rate limit error, got nil")
	}

	// Should have attempted the API call (with auth)
	if requestCount == 0 {
		t.Error("Expected at least one API request")
	}

	// Verify error indicates rate limiting, not auth failure
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("Expected 429 error, got: %s", err.Error())
	}

	t.Logf("Successfully handled rate limit with OAuth2 auth: %v", err)
}
