package graphql_e2e_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TEST 1: OAuth2 token refresh on 401 response
func TestGraphQL_OAuth2_TokenRefresh_On401(t *testing.T) {
	tokenRequests := 0
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests++

		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		grantType := r.FormValue("grant_type")
		var response map[string]interface{}

		if grantType == "refresh_token" {
			// refresh token request
			response = map[string]interface{}{
				"access_token": "new_access_token_456",
				"token_type":   "Bearer",
				"expires_in":   3600,
			}
		} else {
			// first token request
			response = map[string]interface{}{
				"access_token":  "initial_token_123",
				"token_type":    "Bearer",
				"expires_in":    3600,
				"refresh_token": "refresh_token_abc",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlRequests := 0
	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gqlRequests++
		auth := r.Header.Get("Authorization")

		// first request with initial token above returns 401
		if auth == "Bearer initial_token_123" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"errors":[{"message":"Token expired"}]}`))
			return
		}

		// second request with new token succeeds
		if auth == "Bearer new_access_token_456" {
			response := map[string]interface{}{
				"data": map[string]interface{}{
					"viewer": map[string]interface{}{
						"id": "USER-123",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"errors":[{"message":"Invalid token"}]}`))
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-refresh-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields: []config.Field{
						{Name: "id", Path: "id"},
					},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Should succeed after token refresh
	if len(results) != 1 {
		t.Errorf("Expected 1 result after token refresh, got %d", len(results))
	}

	// Should have made 2 token requests (initial + refresh)
	if tokenRequests != 2 {
		t.Errorf("Expected 2 token requests, got %d", tokenRequests)
	}

	// Should have made 2 GraphQL requests (401 + retry)
	if gqlRequests != 2 {
		t.Errorf("Expected 2 GraphQL requests, got %d", gqlRequests)
	}

	t.Logf("Successfully handled token refresh: %d token requests, %d GraphQL calls", tokenRequests, gqlRequests)
}

// TEST 2: Token refresh failure should surface error
func TestGraphQL_OAuth2_TokenRefresh_Failure(t *testing.T) {
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		if r.FormValue("grant_type") == "refresh_token" {
			// Refresh fails
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}

		// Initial token succeeds
		response := map[string]interface{}{
			"access_token":  "initial_token",
			"token_type":    "Bearer",
			"expires_in":    1, // Fast expiry
			"refresh_token": "refresh_token",
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always return 401 to trigger refresh attempts
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"errors":[{"message":"Unauthorized"}]}`))
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-refresh-failure-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
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
		t.Fatal("Expected error when token refresh fails, got nil")
	}

	if !strings.Contains(err.Error(), "token refresh failed") {
		t.Errorf("Expected error to contain 'token refresh failed', got: %v", err)
	}

	t.Logf("Correctly surfaced token refresh failure: %v", err)
}

// TEST 3: Preemptive token refresh based on RefreshBefore margin
func TestGraphQL_OAuth2_PreemptiveRefresh(t *testing.T) {
	tokenRequests := 0
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests++

		response := map[string]interface{}{
			"access_token": fmt.Sprintf("token_%d", tokenRequests),
			"token_type":   "Bearer",
			"expires_in":   2, // 2 second expiry
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"viewer": map[string]interface{}{"id": "123"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-preemptive-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					RefreshBefore: 1, // Refresh 1 second before expiry
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// First request
	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("First extract failed: %v", err)
	}
	firstTokenCount := tokenRequests

	// Wait for token to be within refresh margin (2s expiry - 1s margin = 1s)
	time.Sleep(1500 * time.Millisecond)

	// Second request should trigger preemptive refresh
	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Second extract failed: %v", err)
	}
	secondTokenCount := tokenRequests

	if secondTokenCount <= firstTokenCount {
		t.Errorf("Expected preemptive refresh, but token requests did not increase: %d→%d", firstTokenCount, secondTokenCount)
	}

	t.Logf("Successfully performed preemptive refresh: %d → %d token requests", firstTokenCount, secondTokenCount)
}

// TEST 4: Concurrent requests should share token fetch
func TestGraphQL_OAuth2_ConcurrentRequests(t *testing.T) {
	var tokenRequests int
	var mu sync.Mutex

	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tokenRequests++
		mu.Unlock()

		// Add delay to simulate network latency
		time.Sleep(100 * time.Millisecond)

		response := map[string]interface{}{
			"access_token": "shared_token_123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"viewer": map[string]interface{}{"id": "123"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-concurrent-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// run multiple concurrent requests
	var wg sync.WaitGroup
	numRequests := 5

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := connector.Extract(context.Background())
			if err != nil {
				t.Errorf("Concurrent extract failed: %v", err)
			}
		}()
	}

	wg.Wait()

	// Should only make one token request despite multiple concurrent calls
	if tokenRequests != 1 {
		t.Errorf("Expected 1 token request for concurrent calls, got %d", tokenRequests)
	}

	t.Logf("Successfully shared token across %d concurrent requests", numRequests)
}

// TEST 5: Malformed token response handling
func TestGraphQL_OAuth2_MalformedTokenResponse(t *testing.T) {
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return malformed JSON
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token": "valid", "expires_in": "invalid_number"}`))
	}))
	defer oauth2Mock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-malformed-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: "http://never-called.local",
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
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
		t.Fatal("Expected error for malformed token response, got nil")
	}

	if !strings.Contains(err.Error(), "decode token response") {
		t.Errorf("Expected token decode error, got: %v", err)
	}

	t.Logf("Correctly handled malformed token response: %v", err)
}

// TEST 6: Token endpoint unreachable
func TestGraphQL_OAuth2_TokenEndpointUnreachable(t *testing.T) {
	cfg := &config.Pipeline{
		Name: "graphql-oauth2-unreachable-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: "http://never-called.local",
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     "http://127.0.0.1:54321", // Nothing listening
					ClientID:     "test-client",
					ClientSecret: "test-secret",
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
		t.Fatal("Expected error when token endpoint unreachable, got nil")
	}

	// Updated assertion - look for the actual request error in the chain
	if !strings.Contains(err.Error(), "execute token request") {
		t.Errorf("Expected token request error, got: %v", err)
	}

	t.Logf("Correctly handled unreachable token endpoint: %v", err)
}

// TEST 7: Non 401 errors should NOT trigger token refresh
func TestGraphQL_OAuth2_NoRefreshOnNon401(t *testing.T) {
	tokenRequests := 0
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests++
		response := map[string]interface{}{
			"access_token": "valid_token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 403 (not 401) should not trigger
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"errors":[{"message":"Forbidden"}]}`))
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-no-refresh-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
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
		t.Fatal("Expected error on 403, got nil")
	}

	// Should only have made initial token request, no refresh
	if tokenRequests != 1 {
		t.Errorf("Expected 1 token request (no refresh on 403), got %d", tokenRequests)
	}

	t.Logf("Correctly avoided token refresh on 403 error")
}

// TEST: OAuth2 sends extra parameters when configured
func TestGraphQL_OAuth2_ExtraParams(t *testing.T) {
	var receivedParams map[string]string
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()

		// Capture all form parameters
		receivedParams = make(map[string]string)
		for key, values := range r.Form {
			if len(values) > 0 {
				receivedParams[key] = values[0]
			}
		}

		// Return token
		response := map[string]interface{}{
			"access_token": "test_token_123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"viewer": map[string]interface{}{"id": "123"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-extra-params-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields:   []config.Field{{Name: "id", Path: "id"}},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
					Scope:        "read:user",
					ExtraParams: map[string]string{
						"audience":     "https://api.example.com",
						"custom_param": "custom_value",
						"tenant_id":    "tenant-123",
					},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify all standard params were sent
	expectedParams := map[string]string{
		"grant_type":    "client_credentials",
		"client_id":     "test-client",
		"client_secret": "test-secret",
		"scope":         "read:user",
		// Extra params
		"audience":     "https://api.example.com",
		"custom_param": "custom_value",
		"tenant_id":    "tenant-123",
	}

	for key, expected := range expectedParams {
		if actual, ok := receivedParams[key]; !ok {
			t.Errorf("Missing parameter %s", key)
		} else if actual != expected {
			t.Errorf("Parameter %s: expected '%s', got '%s'", key, expected, actual)
		}
	}

	// Ensure no unexpected params
	if len(receivedParams) != len(expectedParams) {
		t.Errorf("Unexpected parameters sent. Got %d, expected %d", len(receivedParams), len(expectedParams))
		t.Logf("All received params: %+v", receivedParams)
	}

	t.Logf("Successfully sent OAuth2 request with extra parameters")
}
