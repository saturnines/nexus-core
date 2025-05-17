package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// OAuth2Auth implements the interface for OAuth 2.0 authentication
type OAuth2Auth struct {
	// Configuration
	TokenURL      string            // OAuth2 token endpoint URL
	ClientID      string            // OAuth2 client ID
	ClientSecret  string            // OAuth2 client secret
	Scope         string            // Optional scope for the token
	ExtraParams   map[string]string // more parameters for token requests
	RefreshBefore int               // Seconds before expiry to refresh token

	// Token state
	accessToken  string     // current access token
	refreshToken string     // token used to refresh access token
	expiresAt    time.Time  // token expiry time
	mutex        sync.Mutex // prevent concurrent token refreshes
}

// TokenResponse represents the response from the OAuth2 token endpoint
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// NewOAuth2Auth creates a new OAuth2 auth handler
func NewOAuth2Auth(tokenURL, clientID, clientSecret, scope string, extraParams map[string]string, refreshBefore int) (*OAuth2Auth, error) {
	if tokenURL == "" {
		return nil, fmt.Errorf("token URL is required for OAuth2")
	}
	if clientID == "" {
		return nil, fmt.Errorf("client ID is required for OAuth2")
	}
	if clientSecret == "" {
		return nil, fmt.Errorf("client secret is required for OAuth2")
	}

	return &OAuth2Auth{
		TokenURL:      tokenURL,
		ClientID:      clientID,
		ClientSecret:  clientSecret,
		Scope:         scope,
		ExtraParams:   extraParams,
		RefreshBefore: refreshBefore,
	}, nil
}

// ApplyAuth adds the OAuth2 token to the request
func (o *OAuth2Auth) ApplyAuth(req *http.Request) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// Check if we need to get a token (first time or expired)
	if o.accessToken == "" || time.Now().After(o.expiresAt) {
		if err := o.refreshAccessToken(); err != nil {
			return fmt.Errorf("failed to get OAuth2 token: %w", err)
		}
	}

	// apply token to request
	req.Header.Set("Authorization", "Bearer "+o.accessToken)
	return nil
}

// refreshAccessToken gets a new access token using client credentials grant
func (o *OAuth2Auth) refreshAccessToken() error {
	// Prepare the token request
	data := url.Values{}

	// If we have a refresh token use refresh token
	if o.refreshToken != "" {
		data.Set("grant_type", "refresh_token")
		data.Set("refresh_token", o.refreshToken)
	} else {
		// Otherwise use client credentials
		data.Set("grant_type", "client_credentials")
	}

	// add client ID and secret
	data.Set("client_id", o.ClientID)
	data.Set("client_secret", o.ClientSecret)

	// add scope if specified
	if o.Scope != "" {
		data.Set("scope", o.Scope)
	}

	// add any extra parameters
	for key, value := range o.ExtraParams {
		data.Set(key, value)
	}

	// Create and execute the request
	req, err := http.NewRequest("POST", o.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	// check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("token request returned status %d: %s", resp.StatusCode, body)
	}

	// parse the response
	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	// update token state
	o.accessToken = tokenResp.AccessToken

	// store refresh token
	if tokenResp.RefreshToken != "" {
		o.refreshToken = tokenResp.RefreshToken
	}

	// Calculate token expiry
	refreshBefore := 60 // Default 60 seconds if not set
	if o.RefreshBefore > 0 {
		refreshBefore = o.RefreshBefore
	}

	if tokenResp.ExpiresIn > 0 {
		// Use the configurable refresh margin instead of hardcoded 60 seconds
		o.expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn-refreshBefore) * time.Second)
	} else {
		// If no expiry provided, assume 1 hour
		o.expiresAt = time.Now().Add(1 * time.Hour)
	}

	return nil
}

// String returns a string representation of this auth method
func (o *OAuth2Auth) String() string {
	return fmt.Sprintf("OAuth2Auth(client_id: %s, url: %s)", o.ClientID, o.TokenURL)
}
