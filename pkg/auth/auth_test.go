// pkg/connector/api/auth/auth_test.go

package auth

import (
	"encoding/base64"
	"github.com/saturnines/nexus-core/pkg/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Helper functions for tests
func assertHeader(t *testing.T, req *http.Request, header, expected string) {
	t.Helper()
	if value := req.Header.Get(header); value != expected {
		t.Errorf("Expected %s header '%s', got '%s'", header, expected, value)
	}
}

func assertQueryParam(t *testing.T, req *http.Request, param, expected string) {
	t.Helper()
	if value := req.URL.Query().Get(param); value != expected {
		t.Errorf("Expected %s query param '%s', got '%s'", param, expected, value)
	}
}

func assertErrorContains(t *testing.T, err error, expected string) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error containing '%s', got nil", expected)
		return
	}
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected error containing '%s', got '%s'", expected, err.Error())
	}
}

// Test APIKeyAuth
func TestAPIKeyAuth(t *testing.T) {
	t.Run("HeaderBased", func(t *testing.T) {
		auth := NewAPIKeyAuth("X-API-Key", "", "test-api-key")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		assertHeader(t, req, "X-API-Key", "test-api-key")
	})

	t.Run("QueryBased", func(t *testing.T) {
		auth := NewAPIKeyAuth("", "api_key", "test-api-key")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		assertQueryParam(t, req, "api_key", "test-api-key")
	})

	t.Run("BothHeaderAndQuery", func(t *testing.T) {
		auth := NewAPIKeyAuth("X-API-Key", "api_key", "test-api-key")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		assertHeader(t, req, "X-API-Key", "test-api-key")
		assertQueryParam(t, req, "api_key", "test-api-key")
	})

	t.Run("MissingValue", func(t *testing.T) {
		auth := NewAPIKeyAuth("X-API-Key", "", "")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		assertErrorContains(t, err, "API key value is required")
	})

	t.Run("MissingHeaderAndQuery", func(t *testing.T) {
		auth := NewAPIKeyAuth("", "", "test-api-key")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		assertErrorContains(t, err, "requires either header name or query parameter name")
	})

	t.Run("StringMethod", func(t *testing.T) {
		auth := NewAPIKeyAuth("X-API-Key", "", "test-api-key")
		str := auth.String()
		if !strings.Contains(str, "X-API-Key") {
			t.Errorf("String() should contain header name, got: %s", str)
		}
	})
}

// Test BasicAuth
func TestBasicAuth(t *testing.T) {
	t.Run("ValidCredentials", func(t *testing.T) {
		auth := NewBasicAuth("testuser", "testpass")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		encoded := base64.StdEncoding.EncodeToString([]byte("testuser:testpass"))
		assertHeader(t, req, "Authorization", "Basic "+encoded)
	})

	t.Run("EmptyUsername", func(t *testing.T) {
		auth := NewBasicAuth("", "testpass")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		assertErrorContains(t, err, "username is empty")
	})

	t.Run("EmptyPassword", func(t *testing.T) {
		auth := NewBasicAuth("testuser", "")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		// Empty password should still work - some APIs allow this
		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth with empty password failed: %v", err)
		}

		encoded := base64.StdEncoding.EncodeToString([]byte("testuser:"))
		assertHeader(t, req, "Authorization", "Basic "+encoded)
	})

	t.Run("StringMethod", func(t *testing.T) {
		auth := NewBasicAuth("testuser", "testpass")
		str := auth.String()
		if !strings.Contains(str, "testuser") {
			t.Errorf("String() should contain username, got: %s", str)
		}
		if strings.Contains(str, "testpass") {
			t.Errorf("String() should not contain password, got: %s", str)
		}
	})
}

// Test BearerAuth
func TestBearerAuth(t *testing.T) {
	t.Run("ValidToken", func(t *testing.T) {
		auth := NewBearerAuth("test-token")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		assertHeader(t, req, "Authorization", "Bearer test-token")
	})

	t.Run("EmptyToken", func(t *testing.T) {
		auth := NewBearerAuth("")
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		err := auth.ApplyAuth(req)
		assertErrorContains(t, err, "token is empty")
	})

	t.Run("StringMethod", func(t *testing.T) {
		auth := NewBearerAuth("test-token")
		str := auth.String()
		if strings.Contains(str, "test-token") {
			t.Errorf("String() should not contain the actual token, got: %s", str)
		}
	})
}

// Test OAuth2Auth
func TestOAuth2Auth(t *testing.T) {
	t.Run("TokenAcquisition", func(t *testing.T) {
		// Create a mock OAuth2 server
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Verify request method and headers
			if r.Method != "POST" {
				t.Errorf("Expected POST request, got %s", r.Method)
			}
			if contentType := r.Header.Get("Content-Type"); contentType != "application/x-www-form-urlencoded" {
				t.Errorf("Expected Content-Type 'application/x-www-form-urlencoded', got '%s'", contentType)
			}

			// Parse form data
			if err := r.ParseForm(); err != nil {
				t.Errorf("Failed to parse form: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Verify required fields
			if grantType := r.FormValue("grant_type"); grantType != "client_credentials" {
				t.Errorf("Expected grant_type 'client_credentials', got '%s'", grantType)
			}
			if clientID := r.FormValue("client_id"); clientID != "test-client" {
				t.Errorf("Expected client_id 'test-client', got '%s'", clientID)
			}
			if clientSecret := r.FormValue("client_secret"); clientSecret != "test-secret" {
				t.Errorf("Expected client_secret 'test-secret', got '%s'", clientSecret)
			}
			if scope := r.FormValue("scope"); scope != "read write" {
				t.Errorf("Expected scope 'read write', got '%s'", scope)
			}

			// Return mock token response ( Don't need to handle error here)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{ 
				"access_token": "mock-access-token",
				"token_type": "Bearer",
				"expires_in": 3600,
				"refresh_token": "mock-refresh-token",
				"scope": "read write"
			}`))
		}))
		defer mockServer.Close()

		// Create OAuth with the mock URL
		extraParams := map[string]string{
			"extra_param": "extra_value",
		}
		// Add the refreshBefore parameter (60 seconds)
		auth, _ := NewOAuth2Auth(mockServer.URL, "test-client", "test-secret", "read write", extraParams, 60)
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		// Apply auth should get a token from mock
		err := auth.ApplyAuth(req)
		if err != nil {
			t.Fatalf("ApplyAuth failed: %v", err)
		}

		// Check if Authorization header was set correctly
		assertHeader(t, req, "Authorization", "Bearer mock-access-token")
	})

	t.Run("TokenRefresh", func(t *testing.T) {
		// Create a mock server that handles both initial token and refresh
		tokenCount := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := r.ParseForm(); err != nil {
				t.Errorf("Failed to parse form: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Check if this is a token refresh request
			grantType := r.FormValue("grant_type")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			tokenCount++
			if grantType == "refresh_token" {
				// Verify refresh token
				if refreshToken := r.FormValue("refresh_token"); refreshToken != "mock-refresh-token" {
					t.Errorf("Expected refresh_token 'mock-refresh-token', got '%s'", refreshToken)
				}

				// Return new token
				w.Write([]byte(`{
					"access_token": "new-access-token",
					"token_type": "Bearer",
					"expires_in": 3600
				}`))
			} else {
				// Return initial token with short expiry
				w.Write([]byte(`{
					"access_token": "initial-access-token",
					"token_type": "Bearer",
					"expires_in": 1,
					"refresh_token": "mock-refresh-token"
				}`))
			}
		}))
		defer mockServer.Close()

		// Create OAuth2Auth with refreshBefore parameter
		auth, _ := NewOAuth2Auth(mockServer.URL, "test-client", "test-secret", "read", nil, 60)

		// First request should get initial token
		req1, _ := http.NewRequest("GET", "https://api.example.com/data", nil)
		err := auth.ApplyAuth(req1)
		if err != nil {
			t.Fatalf("First ApplyAuth failed: %v", err)
		}
		assertHeader(t, req1, "Authorization", "Bearer initial-access-token")

		// Wait for token to expire
		time.Sleep(2 * time.Second)

		// Second request should use refresh token
		req2, _ := http.NewRequest("GET", "https://api.example.com/data", nil)
		err = auth.ApplyAuth(req2)
		if err != nil {
			t.Fatalf("Second ApplyAuth failed: %v", err)
		}
		assertHeader(t, req2, "Authorization", "Bearer new-access-token")

		// check we got 2 tokens
		if tokenCount != 2 {
			t.Errorf("Expected 2 token requests, got %d", tokenCount)
		}
	})

	t.Run("TokenServerError", func(t *testing.T) {
		// Create a mock server that returns an error
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid_client"}`))
		}))
		defer mockServer.Close()

		// Create OAuth2Auth with refreshBefore parameter
		auth, _ := NewOAuth2Auth(mockServer.URL, "bad-client", "bad-secret", "", nil, 60)
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		// Apply auth should fail here
		err := auth.ApplyAuth(req)
		assertErrorContains(t, err, "token request returned status 401")
	})

	t.Run("StringMethod", func(t *testing.T) {
		auth, _ := NewOAuth2Auth("https://auth.example.com/token", "client-id", "client-secret", "read", nil, 60)
		str := auth.String()
		if !strings.Contains(str, "client-id") {
			t.Errorf("String() should contain client ID, got: %s", str)
		}
		if strings.Contains(str, "client-secret") {
			t.Errorf("String() should not contain client secret, got: %s", str)
		}
	})
}

// Test for the factory function
func TestCreateHandler(t *testing.T) {
	// Define a simple test for the factory function to get started
	t.Run("BasicAuthCreation", func(t *testing.T) {
		authConfig := &config.Auth{
			Type: config.AuthTypeBasic,
			Basic: &config.BasicAuth{
				Username: "user",
				Password: "pass",
			},
		}

		handler, err := CreateHandler(authConfig)
		if err != nil {
			t.Fatalf("CreateHandler failed: %v", err)
		}

		basicAuth, ok := handler.(*BasicAuth)
		if !ok {
			t.Fatalf("Expected *BasicAuth, got %T", handler)
		}

		if basicAuth.Username != "user" || basicAuth.Password != "pass" {
			t.Errorf("Auth not properly configured: %+v", basicAuth)
		}
	})

	// For future reference If I were to add new tests just add more tests for diff auth types
	t.Run("APIKeyAuthCreation", func(t *testing.T) {
		authConfig := &config.Auth{
			Type: config.AuthTypeAPIKey,
			APIKey: &config.APIKeyAuth{
				Header:     "X-API-Key",
				QueryParam: "",
				Value:      "test-api-key",
			},
		}

		handler, err := CreateHandler(authConfig)
		if err != nil {
			t.Fatalf("CreateHandler failed: %v", err)
		}

		apiKeyAuth, ok := handler.(*APIKeyAuth)
		if !ok {
			t.Fatalf("Expected *APIKeyAuth, got %T", handler)
		}

		if apiKeyAuth.HeaderName != "X-API-Key" || apiKeyAuth.Value != "test-api-key" {
			t.Errorf("Auth not properly configured: %+v", apiKeyAuth)
		}
	})

	t.Run("BearerAuthCreation", func(t *testing.T) {
		authConfig := &config.Auth{
			Type: config.AuthTypeBearer,
			Bearer: &config.BearerAuth{
				Token: "test-token",
			},
		}

		handler, err := CreateHandler(authConfig)
		if err != nil {
			t.Fatalf("CreateHandler failed: %v", err)
		}

		bearerAuth, ok := handler.(*BearerAuth)
		if !ok {
			t.Fatalf("Expected *BearerAuth, got %T", handler)
		}

		if bearerAuth.Token != "test-token" {
			t.Errorf("Auth not properly configured: %+v", bearerAuth)
		}
	})

	t.Run("OAuth2AuthCreation", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
                "access_token": "test-access-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }`))
		}))
		defer mockServer.Close()

		authConfig := &config.Auth{
			Type: config.AuthTypeOAuth2,
			OAuth2: &config.OAuth2Auth{
				TokenURL:     mockServer.URL,
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Scope:        "read write",
			},
		}

		handler, err := CreateHandler(authConfig)
		if err != nil {
			t.Fatalf("CreateHandler failed: %v", err)
		}

		oauth2Auth, ok := handler.(*OAuth2Auth)
		if !ok {
			t.Fatalf("Expected *OAuth2Auth, got %T", handler)
		}

		if oauth2Auth.ClientID != "client-id" || oauth2Auth.TokenURL != mockServer.URL {
			t.Errorf("Auth not properly configured: %+v", oauth2Auth)
		}
	})

	t.Run("UnsupportedAuthType", func(t *testing.T) {
		authConfig := &config.Auth{
			Type: config.AuthType("unsupported"),
		}

		_, err := CreateHandler(authConfig)
		if err == nil {
			t.Fatal("Expected error for unsupported auth type, got nil")
		}
	})
}

// Add a test for the new registry based  capability
func TestRegisterAuthHandler(t *testing.T) {

	customType := config.AuthType("custom")

	// Register a handler
	RegisterAuthHandler(customType, func(cfg *config.Auth) (Handler, error) {
		return NewBasicAuth("custom", "secret"), nil
	})

	// Configure auth with custom type
	authConfig := &config.Auth{
		Type: customType,
	}

	// Try to create the handler
	handler, err := CreateHandler(authConfig)
	if err != nil {
		t.Fatalf("Failed to create custom handler: %v", err)
	}

	// Verify the handler
	customHandler, ok := handler.(*BasicAuth)
	if !ok {
		t.Fatal("Custom handler is not a BasicAuth")
	}

	if customHandler.Username != "custom" || customHandler.Password != "secret" {
		t.Error("Custom handler has incorrect values")
	}
}
