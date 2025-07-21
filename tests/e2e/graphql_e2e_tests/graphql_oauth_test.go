package graphql_e2e_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"github.com/saturnines/nexus-core/pkg/errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// / TEST: GraphQL with OAuth2 authentication
func TestGraphQL_OAuth2Authentication(t *testing.T) {
	// Setup OAuth2 mock server
	tokenRequests := 0
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests++

		// Check it's a proper OAuth2 token request
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Check grant_type
		if grantType := r.FormValue("grant_type"); grantType != "client_credentials" {
			t.Errorf("Expected grant_type='client_credentials', got '%s'", grantType)
		}

		// Check client credentials
		if clientID := r.FormValue("client_id"); clientID != "graphql-client" {
			t.Errorf("Expected client_id='graphql-client', got '%s'", clientID)
		}

		response := map[string]interface{}{
			"access_token": "gql_access_token_123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer oauth2Mock.Close()

	// Setup GraphQL server that requires auth
	gqlRequests := 0
	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gqlRequests++

		// Parse GraphQL request
		var gqlReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&gqlReq); err != nil {
			t.Errorf("Failed to parse GraphQL request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify Authorization header
		auth := r.Header.Get("Authorization")
		if auth != "Bearer gql_access_token_123" {
			t.Errorf("Expected Authorization 'Bearer gql_access_token_123', got '%s'", auth)
		}

		// Return successful response (no errors field!)
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"viewer": map[string]interface{}{
					"id":       "USER-123",
					"username": "testuser",
					"email":    "test@example.com",
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id username email } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "username", Path: "username"},
						{Name: "email", Path: "email"},
					},
				},
			},
		},
	}

	// Set Auth at the Source level, not inside GraphQLConfig
	cfg.Source.Auth = &config.Auth{
		Type: config.AuthTypeOAuth2,
		OAuth2: &config.OAuth2Auth{
			TokenURL:     oauth2Mock.URL,
			ClientID:     "graphql-client",
			ClientSecret: "graphql-secret",
			Scope:        "read:user",
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

	// Verify results
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 {
		user := results[0]
		if user["id"] != "USER-123" {
			t.Errorf("Expected id='USER-123', got %v", user["id"])
		}
		if user["username"] != "testuser" {
			t.Errorf("Expected username='testuser', got %v", user["username"])
		}
		if user["email"] != "test@example.com" {
			t.Errorf("Expected email='test@example.com', got %v", user["email"])
		}
	}

	// Verify OAuth2 flow happened
	if tokenRequests != 1 {
		t.Errorf("Expected 1 token request, got %d", tokenRequests)
	}

	if gqlRequests != 1 {
		t.Errorf("Expected 1 GraphQL request, got %d", gqlRequests)
	}

	t.Logf("Successfully authenticated GraphQL request via OAuth2")
}

// TEST: GraphQL OAuth2 authentication failure
func TestGraphQL_OAuth2Authentication_Failure(t *testing.T) {
	// Setup OAuth2 mock server that denies access
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"error": "invalid_client", "error_description": "Client authentication failed"}`))
	}))
	defer oauth2Mock.Close()

	// GraphQL server (shouldn't be reached)
	gqlCalled := false
	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gqlCalled = true
		t.Error("GraphQL endpoint should not be called when OAuth2 fails")
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-failure-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { viewer { id } }`,
				Auth: &config.Auth{
					Type: config.AuthTypeOAuth2,
					OAuth2: &config.OAuth2Auth{
						TokenURL:     oauth2Mock.URL,
						ClientID:     "invalid-client",
						ClientSecret: "invalid-secret",
					},
				},
				ResponseMapping: config.ResponseMapping{
					RootPath: "viewer",
					Fields: []config.Field{
						{Name: "id", Path: "id"},
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
	if err == nil {
		t.Fatal("Expected OAuth2 auth failure, got nil")
	}

	// Verify error is authentication related
	if !errors.Is(err, errors.ErrAuthentication) {
		t.Errorf("Expected ErrAuthentication, got error type: %T", err)
	}

	// Verify GraphQL was never called
	if gqlCalled {
		t.Error("GraphQL endpoint should not be called when OAuth2 fails")
	}

	t.Logf("Correctly handled OAuth2 authentication failure: %v", err)
}

// TEST: OAuth2 sends scope when configured
// TEST: OAuth2 sends scope when configured
func TestGraphQL_OAuth2ScopeParameter(t *testing.T) {
	var gotScope string
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		gotScope = r.FormValue("scope")
		resp := map[string]interface{}{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"dummy": map[string]interface{}{
					"value": 1,
				},
			},
		})
	}))
	defer gqlMock.Close()

	cfg := &config.Pipeline{
		Name: "graphql-oauth2-scope-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlMock.URL,
				Query:    `query { dummy { value } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "dummy",
					Fields: []config.Field{
						{Name: "value", Path: "value"},
					},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     oauth2Mock.URL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
					Scope:        "read:user write:user", // Test scope parameter
				},
			},
		},
	}

	conn, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = conn.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if gotScope != "read:user write:user" {
		t.Errorf("Expected scope='read:user write:user', got '%s'", gotScope)
	}
}

// Helper function for below test
func createTestGraphQLOAuth2Config(tokenURL, gqlURL string) *config.Pipeline {
	return &config.Pipeline{
		Name: "graphql-oauth2-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: gqlURL,
				Query:    `query { dummy { value } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "dummy",
					Fields: []config.Field{
						{Name: "value", Path: "value"},
					},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeOAuth2,
				OAuth2: &config.OAuth2Auth{
					TokenURL:     tokenURL,
					ClientID:     "test-client",
					ClientSecret: "test-secret",
				},
			},
		},
	}
}

// TEST: Token is fetched once, then cached
func TestGraphQL_OAuth2TokenCaching(t *testing.T) {
	var calls int
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		resp := map[string]interface{}{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"dummy": map[string]interface{}{"value": 1},
			},
		})
	}))
	defer gqlMock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, gqlMock.URL)

	conn, _ := core.NewConnector(cfg)
	ctx := context.Background()

	conn.Extract(ctx)
	conn.Extract(ctx)

	if calls != 1 {
		t.Errorf("Expected 1 token fetch, got %d", calls)
	}
}

// TEST: Token expiry triggers new fetch
func TestGraphQL_OAuth2TokenExpiry(t *testing.T) {
	var calls int
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   1, // 1 second
		})
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"dummy": 1},
		})
	}))
	defer gqlMock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, gqlMock.URL)

	conn, _ := core.NewConnector(cfg)
	ctx := context.Background()

	conn.Extract(ctx)
	time.Sleep(1100 * time.Millisecond)
	conn.Extract(ctx)

	if calls < 2 {
		t.Errorf("Expected fresh token after expiry, got %d calls", calls)
	}
}

// TEST: Malformed token payload
func TestGraphQL_OAuth2MalformedTokenResponse(t *testing.T) {
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"token_type":"Bearer"}`)) // missing access_token
	}))
	defer oauth2Mock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, "http://never-usedxd")

	conn, _ := core.NewConnector(cfg)
	_, err := conn.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected parse error on malformed token")
	}
}

// TEST: Token endpoint server error
func TestGraphQL_OAuth2TokenEndpointError(t *testing.T) {
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer oauth2Mock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, "http://never-usedxd")

	conn, _ := core.NewConnector(cfg)
	_, err := conn.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error on 500 from token endpoint")
	}
}

// TEST: 401 from GraphQL causes refresh & retry
func TestGraphQL_OAuth2GraphQLUnauthorizedRetry(t *testing.T) {
	var tokenCalls, gqlCalls int
	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls++
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": fmt.Sprintf("tok%d", tokenCalls),
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gqlCalls++
		if gqlCalls == 1 {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"dummy": map[string]interface{}{
					"value": 2,
				},
			},
		})
	}))
	defer gqlMock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, gqlMock.URL)

	conn, _ := core.NewConnector(cfg)
	res, err := conn.Extract(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("Expected data after retry")
	}
	if tokenCalls < 2 {
		t.Errorf("Expected second token fetch on 401, got %d", tokenCalls)
	}
}

// TEST: Concurrent Extract calls share token fetch
func TestGraphQL_OAuth2ConcurrentExtract(t *testing.T) {
	var mu sync.Mutex
	var calls int

	oauth2Mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer oauth2Mock.Close()

	gqlMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"dummy": 3},
		})
	}))
	defer gqlMock.Close()

	cfg := createTestGraphQLOAuth2Config(oauth2Mock.URL, gqlMock.URL)

	conn, _ := core.NewConnector(cfg)
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.Extract(context.Background())
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Errorf("Expected one token fetch across 5 goroutines, got %d", calls)
	}
}
