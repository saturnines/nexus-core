package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// APIClient struct
type APIClient struct {
	httpClient *http.Client
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

// WithTimeout time out
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *APIClient) {
		c.httpClient.Timeout = timeout
	}
}

// WithHeader adds a header to all requests
func WithHeader(key, value string) ClientOption {
	return func(c *APIClient) {
		c.headers[key] = value
	}
}

// Get performs a GET request to the specified endpoint
func (c *APIClient) Get(endpoint string) (*http.Response, error) {
	return c.Request(http.MethodGet, endpoint, nil)
}

// Request performs an HTTP request with whatever method to the endpoint
func (c *APIClient) Request(method, endpoint string, body []byte) (*http.Response, error) {
	url := fmt.Sprintf("%s%s", c.baseURL, endpoint)

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, bodyReader)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add default headers
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}

	// All requests with a body are JSON, set Content-Type header
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.httpClient.Do(req)
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

// ExtractField extracts a field from a map using a simple path
func ExtractField(data map[string]interface{}, path string) (interface{}, bool) {
	value, ok := data[path]
	return value, ok
}
