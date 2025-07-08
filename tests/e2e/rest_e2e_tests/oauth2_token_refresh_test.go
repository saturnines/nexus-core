package rest_e2e_tests_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
)

// Helper struct to track OAuth2 server state
type OAuth2MockServer struct {
	Server            *httptest.Server
	TokenRequests     []TokenRequest
	RefreshCount      int
	ShouldFailRefresh bool
	TokenLifetime     int // seconds
}

type TokenRequest struct {
	GrantType    string
	ClientID     string
	ClientSecret string
	RefreshToken string
	Timestamp    time.Time
}

func NewOAuth2MockServer() *OAuth2MockServer {
	mock := &OAuth2MockServer{
		TokenLifetime: 1, // Very short for testing
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.handleTokenRequest(w, r)
	}))

	return mock
}

func (m *OAuth2MockServer) handleTokenRequest(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	req := TokenRequest{
		GrantType:    r.FormValue("grant_type"),
		ClientID:     r.FormValue("client_id"),
		ClientSecret: r.FormValue("client_secret"),
		RefreshToken: r.FormValue("refresh_token"),
		Timestamp:    time.Now(),
	}
	m.TokenRequests = append(m.TokenRequests, req)

	// Handle refresh token requests
	if req.GrantType == "refresh_token" {
		m.RefreshCount++

		if m.ShouldFailRefresh {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "invalid_grant", "error_description": "refresh token expired"}`))
			return
		}
	}

	// Return success response
	response := map[string]interface{}{
		"access_token":  fmt.Sprintf("token_%d_%d", len(m.TokenRequests), time.Now().Unix()),
		"token_type":    "Bearer",
		"expires_in":    m.TokenLifetime,
		"refresh_token": "refresh_token_123",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *OAuth2MockServer) Close() {
	m.Server.Close()
}

// Helper struct for API mock server
type APIMockServer struct {
	Server          *httptest.Server
	RequestCount    int
	ShouldReturn401 bool
	Return401Count  int
}

func NewAPIMockServer() *APIMockServer {
	mock := &APIMockServer{}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.handleAPIRequest(w, r)
	}))

	return mock
}

func (m *APIMockServer) handleAPIRequest(w http.ResponseWriter, r *http.Request) {
	m.RequestCount++

	// Check for Authorization header
	auth := r.Header.Get("Authorization")
	if auth == "" {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "missing authorization"}`))
		return
	}

	// Simulate 401 on first request to trigger token refresh
	if m.ShouldReturn401 && m.RequestCount <= m.Return401Count {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "token expired"}`))
		return
	}

	// Return successful response
	response := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"id":           m.RequestCount,
				"name":         fmt.Sprintf("Item %d", m.RequestCount),
				"request_auth": auth,
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *APIMockServer) Close() {
	m.Server.Close()
}

// TEST 1: Successful token refresh
func TestConnector_OAuth2_TokenRefresh_Success(t *testing.T) {
	// Setup OAuth2 mock server
	oauth2Mock := NewOAuth2MockServer()
	defer oauth2Mock.Close()

	// Setup API mock server that returns 401 first, then 200
	apiMock := NewAPIMockServer()
	apiMock.ShouldReturn401 = true
	apiMock.Return401Count = 1 // Return 401 on first request only
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-refresh-success-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.Server.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					Scope:         "read",
					RefreshBefore: 0, // Refresh immediately when expired
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx := context.Background()
	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify extraction succeeded
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	// Verify token refresh was triggered
	if oauth2Mock.RefreshCount != 1 {
		t.Errorf("Expected 1 token refresh, got %d", oauth2Mock.RefreshCount)
	}

	// Verify API was called twice (401 then retry with new token)
	if apiMock.RequestCount != 2 {
		t.Errorf("Expected 2 API requests (401 + retry), got %d", apiMock.RequestCount)
	}

	// Verify we got at least 2 token requests (initial + refresh)
	if len(oauth2Mock.TokenRequests) < 2 {
		t.Errorf("Expected at least 2 token requests, got %d", len(oauth2Mock.TokenRequests))
	}

	t.Logf("Successfully handled token refresh: %d token requests, %d API calls",
		len(oauth2Mock.TokenRequests), apiMock.RequestCount)
}

// TEST 2: Token refresh failure
func TestConnector_OAuth2_TokenRefresh_Failure(t *testing.T) {
	// Setup OAuth2 mock server that fails refresh
	oauth2Mock := NewOAuth2MockServer()
	oauth2Mock.ShouldFailRefresh = true
	defer oauth2Mock.Close()

	// Setup API mock server that returns 401
	apiMock := NewAPIMockServer()
	apiMock.ShouldReturn401 = true
	apiMock.Return401Count = 10 // Always return 401
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-refresh-failure-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.Server.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					Scope:         "read",
					RefreshBefore: 0,
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

	ctx := context.Background()
	_, err = connector.Extract(ctx)

	// Should fail with token expired error
	if err == nil {
		t.Fatal("Expected error when token refresh fails, got nil")
	}

	// Check if error contains token refresh failure (more flexible than exact type match)
	errStr := err.Error()
	if !strings.Contains(errStr, "token refresh failed") {
		t.Errorf("Expected error to contain 'token refresh failed', got: %v", err)
	}

	// Verify refresh was attempted
	if oauth2Mock.RefreshCount == 0 {
		t.Error("Expected token refresh to be attempted")
	}

	t.Logf("Correctly surfaced token refresh failure: %v", err)
}

// TEST 3: Token expiry during pagination
func TestConnector_OAuth2_TokenExpiry_DuringPagination(t *testing.T) {
	oauth2Mock := NewOAuth2MockServer()
	defer oauth2Mock.Close()

	requestCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Return 401 on the second request (during pagination)
		if requestCount == 2 {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "token expired"}`))
			return
		}

		// First and third requests succeed
		var response map[string]interface{}
		if requestCount == 1 {
			response = map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "name": "Item 1"},
				},
				"has_more": true,
			}
		} else {
			response = map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 2, "name": "Item 2"},
				},
				"has_more": false,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-pagination-expiry-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.Server.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
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

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Should complete pagination successfully after token refresh
	if len(results) != 2 {
		t.Errorf("Expected 2 results from pagination, got %d", len(results))
	}

	// Should have made 3 requests: page1 (success) → page2 (401) → page2 (retry success)
	if requestCount != 3 {
		t.Errorf("Expected 3 requests (with retry), got %d", requestCount)
	}

	t.Logf("Successfully handled token expiry during pagination: %d requests, %d results",
		requestCount, len(results))
}

// TEST 4: Multiple concurrent token refresh attempts
func TestConnector_OAuth2_ConcurrentRefresh_Prevention(t *testing.T) {
	oauth2Mock := NewOAuth2MockServer()
	oauth2Mock.TokenLifetime = 300 // 5 minutes
	defer oauth2Mock.Close()

	// Reset the mock between tests
	oauth2Mock.RefreshCount = 0
	oauth2Mock.TokenRequests = nil

	// API mock that returns 401 on first request only
	apiMock := NewAPIMockServer()
	apiMock.ShouldReturn401 = true
	apiMock.Return401Count = 1 // Only first request returns 401
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-concurrent-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.Server.URL,
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

	// Make the first request (this should trigger: initial token + 401 + refresh)
	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("First extract failed: %v", err)
	}

	// Record state after first request completes
	tokensAfterFirst := len(oauth2Mock.TokenRequests)
	refreshesAfterFirst := oauth2Mock.RefreshCount

	// Make additional requests immediately (token should still be valid)
	for i := 0; i < 2; i++ {
		_, err := connector.Extract(context.Background())
		if err != nil {
			t.Fatalf("Extract %d failed: %v", i+2, err)
		}
	}

	// Check final counts
	finalTokenRequests := len(oauth2Mock.TokenRequests)
	finalRefreshCount := oauth2Mock.RefreshCount

	// Should not have any additional token requests (token should be reused)
	if finalTokenRequests > tokensAfterFirst {
		t.Errorf("Token was unnecessarily refreshed: expected %d total requests, got %d",
			tokensAfterFirst, finalTokenRequests)
		t.Errorf("This suggests the token expired between requests")
	}

	// Should not have additional refreshes
	if finalRefreshCount > refreshesAfterFirst {
		t.Errorf("Unnecessary token refreshes: expected %d total refreshes, got %d",
			refreshesAfterFirst, finalRefreshCount)
	}

	t.Logf("Successfully reused token: %d total token requests, %d refreshes",
		finalTokenRequests, finalRefreshCount)
}

// ADD: Helper to reset test state
func (m *OAuth2MockServer) Reset() {
	m.RefreshCount = 0
	m.TokenRequests = nil
	m.ShouldFailRefresh = false
}

func (m *APIMockServer) Reset() {
	m.RequestCount = 0
	m.ShouldReturn401 = false
	m.Return401Count = 0
}

// TEST 5: Malformed token JSON from token endpoint
func TestConnector_OAuth2_MalformedTokenResponse(t *testing.T) {
	// Setup OAuth2 mock server that returns invalid JSON
	oauth2Mock := NewOAuth2MockServer()
	oauth2Mock.TokenLifetime = 0
	oauth2Mock.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token": "abc", "expires_in": "oops",}`)) // invalid JSON
	})
	defer oauth2Mock.Close()

	// API mock just returns 200 if called (should never be called)
	apiMock := NewAPIMockServer()
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-malformed-token-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.Server.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					RefreshBefore: 0,
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{{Name: "id", Path: "id"}},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error for malformed token JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode token response") {
		t.Errorf("Expected JSON decode error, got: %v", err)
	}
}

// TEST 6: Preemptive refresh using RefreshBefore margin
func TestConnector_OAuth2_PreemptiveRefresh(t *testing.T) {
	// Token lifetime of 5 seconds, RefreshBefore=4 seconds → token should expire after 1s
	oauth2Mock := NewOAuth2MockServer()
	oauth2Mock.TokenLifetime = 5
	defer oauth2Mock.Close()

	apiMock := NewAPIMockServer()
	apiMock.ShouldReturn401 = false
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-preemptive-refresh-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.Server.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					RefreshBefore: 4,
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{{Name: "id", Path: "id"}},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// First Extract: fetch token thrice? Actually only once
	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("First extract failed: %v", err)
	}
	firstCount := len(oauth2Mock.TokenRequests)

	// Sleep 2 seconds (total token age=2s, expiry at 1s + margin), so token is already expired
	time.Sleep(2 * time.Second)

	// Second Extract: should trigger a new refresh
	_, err = connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Second extract failed: %v", err)
	}
	secondCount := len(oauth2Mock.TokenRequests)

	if secondCount <= firstCount {
		t.Errorf("Expected a pre-emptive refresh, but token requests did not increase: %d→%d", firstCount, secondCount)
	}
}

// TEST 7: API returns non‐401 error—should not refresh token
func TestConnector_OAuth2_NoRefreshOnNon401(t *testing.T) {
	oauth2Mock := NewOAuth2MockServer()
	defer oauth2Mock.Close()

	requestCount := 0
	apiMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Return 403 (forbidden) on first call; connector should NOT refresh
		if requestCount == 1 {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error": "forbidden"}`))
			return
		}
		// Return 200 on retry if it ever tries
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"items": []interface{}{map[string]interface{}{"id": 1}},
		})
	}))
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-norefresh-non401-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.Server.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{{Name: "id", Path: "id"}},
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
	// Check that no refresh occurred
	if oauth2Mock.RefreshCount != 0 {
		t.Errorf("Expected zero token refreshes on non-401, got %d", oauth2Mock.RefreshCount)
	}
}

// TEST 8: HTTP error when contacting TokenURL
func TestConnector_OAuth2_TokenEndpointUnreachable(t *testing.T) {
	// Point to a port where nothing listens
	badTokenURL := "http://127.0.0.1:54321"
	apiMock := NewAPIMockServer()
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-token-unreachable-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.Server.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     badTokenURL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{{Name: "id", Path: "id"}},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error when token endpoint is unreachable, got nil")
	}
	if !strings.Contains(err.Error(), "token request failed") {
		t.Errorf("Expected token request error, got: %v", err)
	}
}

func TestConnector_OAuth2_ConcurrentRefresh_TrueConcurrency(t *testing.T) {
	// Token endpoint that counts requests
	oauth2Mock := NewOAuth2MockServer()
	oauth2Mock.TokenLifetime = 300 // token lives long enough
	defer oauth2Mock.Close()

	// Simple API that always returns a valid JSON payload
	apiMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[{"id":1,"name":"X"}]}`))
	}))
	defer apiMock.Close()

	cfg := &config.Pipeline{
		Name: "oauth2-concurrent-real-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: apiMock.URL,
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:      oauth2Mock.Server.URL,
					ClientID:      "test-client",
					ClientSecret:  "test-secret",
					RefreshBefore: 0, // no pre‐emptive refresh
				},
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{{Name: "id", Path: "id"}},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Launch several goroutines that all call Extract at once
	var wg sync.WaitGroup
	start := make(chan struct{})
	n := 5
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			// We ignore the results; we only care about token fetches
			connector.Extract(context.Background())
		}()
	}

	// Let them all proceed simultaneously
	close(start)
	wg.Wait()

	// Only one token request should have been made
	if got := len(oauth2Mock.TokenRequests); got != 1 {
		t.Errorf("expected 1 token request, got %d", got)
	}
}
