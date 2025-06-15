package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/transport/rest"
	"io"
	"net/http"
	"strings"
	"time"
)

// APIClient struct
type APIClient struct {
	httpClient rest.HTTPDoer
	baseURL    string
	headers    map[string]string
	rateLimit  int
}

// ClientOption defines config for APIClient
type ClientOption func(*APIClient)

// NewClient creates a new APIClient with the given options
func NewClient(baseURL string, options ...ClientOption) *APIClient {
	client := &APIClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
		headers: make(map[string]string),
	}

	// apply all options from config
	for _, option := range options {
		option(client)
	}

	return client
}

func WithClientHTTPOptions(options ...rest.HTTPClientOption) ClientOption {
	return func(c *APIClient) {
		c.httpClient = rest.ApplyHTTPClientOptions(c.httpClient, options...)
	}
}

// WithHeader adds a header to all requests
func WithHeader(key, value string) ClientOption {
	return func(c *APIClient) {
		c.headers[key] = value
	}
}

// Get performs a GET request to the specified endpoint
func (c *APIClient) Get(ctx context.Context, endpoint string) (*http.Response, error) {
	return c.Request(ctx, http.MethodGet, endpoint, nil)
}

// Post performs a POST request with JSON body
func (c *APIClient) Post(ctx context.Context, endpoint string, body []byte) (*http.Response, error) {
	return c.Request(ctx, http.MethodPost, endpoint, body)
}

// Request performs an HTTP request with whatever method to the endpoint
func (c *APIClient) Request(ctx context.Context, method, endpoint string, body []byte) (*http.Response, error) {
	// Use the shared RequestHelper
	return rest.RequestHelper(ctx, c.httpClient, method, c.baseURL, endpoint, c.headers, body)
}

// ExtractJSON extracts JSON data from an HTTP response into the provided target
func ExtractJSON(resp *http.Response, target interface{}) error {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}

// ExtractField extracts a field from a map using a dotted path
func ExtractField(data map[string]interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	// Simple case - no dots
	if !strings.Contains(path, ".") {
		value, ok := data[path]
		return value, ok
	}

	// Nested case - traverse the path
	parts := strings.Split(path, ".")
	var current interface{} = data

	for _, part := range parts {
		currentMap, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}

		current, ok = currentMap[part]
		if !ok {
			return nil, false
		}
	}

	return current, true
}
