package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/transport/graphql"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/saturnines/nexus-core/pkg/auth"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors"
	"github.com/saturnines/nexus-core/pkg/pagination"
	"github.com/saturnines/nexus-core/pkg/transport/rest"
)

// Connector orchestrates requests, pagination and handling.
type Connector struct {
	builder     RequestBuilder
	client      *http.Client
	cfg         *config.Pipeline
	authHandler auth.Handler
	factory     *pagination.Factory
}

// ConnectorOption allows customizing Connector.
type ConnectorOption func(*Connector)

// NewConnector creates a connector based on config
func NewConnector(cfg *config.Pipeline, opts ...ConnectorOption) (*Connector, error) {
	var (
		transport   = http.DefaultTransport
		authHandler auth.Handler
	)

	// retry layer
	if cfg.RetryConfig != nil {
		transport = NewRetryTransport(transport, cfg.RetryConfig)
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// if auth configured, build handler and maybe wrap transport
	if cfg.Source.Auth != nil {
		h, err := auth.CreateHandler(cfg.Source.Auth)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrAuthentication, "auth handler")
		}
		if oauth2Auth, ok := h.(*auth.OAuth2Auth); ok {
			httpClient.Transport = auth.NewOAuth2RoundTripper(httpClient.Transport, oauth2Auth)
			authHandler = nil
		} else {
			authHandler = h
		}
	}

	var builder RequestBuilder

	switch cfg.Source.Type {
	case config.SourceTypeREST:
		builder = rest.NewBuilder(
			cfg.Source.Endpoint,
			cfg.Source.Method,
			cfg.Source.Headers,
			cfg.Source.QueryParams,
			authHandler,
		)

	case config.SourceTypeGraphQL:
		// GraphQLConfig must be non-nil in your config types
		g := cfg.Source.GraphQLConfig
		if g == nil {
			return nil, fmt.Errorf("graphql config missing")
		}
		builder = graphql.NewBuilder(
			g.Endpoint,
			g.Query,
			g.Variables,
			g.Headers,
			authHandler,
		)

	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Source.Type)
	}

	conn := &Connector{
		builder:     builder,
		client:      httpClient,
		cfg:         cfg,
		authHandler: authHandler,
		factory:     pagination.DefaultFactory,
	}

	for _, opt := range opts {
		opt(conn)
	}

	return conn, nil
}

// Extract runs the extraction process
func (c *Connector) Extract(ctx context.Context) ([]map[string]interface{}, error) {
	var allResults []map[string]interface{}

	// If no pagination configured: single-request path
	if c.cfg.Pagination == nil {
		req, err := c.builder.Build(ctx)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "build request") // ✅ Fixed
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "http do") // ✅ Fixed
		}

		batch, err := c.handleResponse(resp) // ✅ This method is defined below
		if err != nil {
			return nil, err
		}

		return batch, nil
	}

	// Pagination path
	pager, err := c.createPager(ctx)
	if err != nil {
		return nil, errors.WrapError(err, errors.ErrPagination, "create pager")
	}

	for {
		req, err := pager.NextRequest()
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrPagination, "get next request")
		}
		if req == nil {
			break // No more pages
		}

		// Apply auth if we have a handler
		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, c.handleAuthError(err)
			}
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "http do")
		}

		// Read body once
		bodyBytes, err := c.readResponseBody(resp)
		if err != nil {
			return nil, err
		}

		// Create buffered response for pager
		bufferedResp := c.createBufferedResponse(resp, bodyBytes)

		// Update pager state
		if err := pager.UpdateState(bufferedResp); err != nil {
			return nil, errors.WrapError(err, errors.ErrPagination, "update pager state")
		}

		// Process response for item extraction
		items, err := c.processResponseFromBytes(resp.StatusCode, bodyBytes)
		if err != nil {
			return nil, err
		}

		pageResults, err := c.extractFields(items)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrExtraction, "extract fields")
		}

		allResults = append(allResults, pageResults...)
	}

	return allResults, nil
}

// createPager creates a pager based on pagination config
func (c *Connector) createPager(ctx context.Context) (pagination.Pager, error) {
	if c.cfg.Pagination == nil {
		return nil, nil
	}

	req, err := c.builder.Build(ctx)
	if err != nil {
		return nil, err
	}

	opts := c.paginationConfigToPagerOptions()
	return c.factory.CreatePager(
		string(c.cfg.Pagination.Type),
		c.client,
		req,
		opts,
	)
}

// paginationConfigToPagerOptions converts config to pager options
func (c *Connector) paginationConfigToPagerOptions() map[string]interface{} {
	opts := make(map[string]interface{})
	p := c.cfg.Pagination

	switch p.Type {
	case config.PaginationTypePage:
		opts["pageParam"] = p.PageParam
		opts["sizeParam"] = p.SizeParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalPagesPath"] = p.TotalPagesPath
		opts["startPage"] = 1
		opts["pageSize"] = p.PageSize
	case config.PaginationTypeOffset:
		opts["offsetParam"] = p.OffsetParam
		opts["sizeParam"] = p.LimitParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalCountPath"] = p.TotalCountPath
		opts["initOffset"] = 0
		opts["pageSize"] = p.OffsetIncrement
	case config.PaginationTypeCursor:
		opts["cursorParam"] = p.CursorParam
		opts["nextPath"] = p.CursorPath
	}

	return opts
}

// handleResponse processes a single HTTP response
func (c *Connector) handleResponse(resp *http.Response) ([]map[string]interface{}, error) {
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.WrapError(fmt.Errorf("API returned status %d", resp.StatusCode), errors.ErrHTTPResponse, "unexpected status code")
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WrapError(err, errors.ErrHTTPResponse, "failed to read response body")
	}

	items, err := c.processResponseFromBytes(resp.StatusCode, bodyBytes)
	if err != nil {
		return nil, err
	}

	return c.extractFields(items)
}

// handleAuthError wraps authentication errors appropriately.
func (c *Connector) handleAuthError(err error) error {
	var tr *auth.TokenRefreshError
	if errors.As(err, &tr) {
		return errors.WrapError(err, errors.ErrTokenExpired, "token refresh failed")
	}
	return errors.WrapError(err, errors.ErrAuthentication, "authentication failed")
}

// readResponseBody safely reads and closes the response body
func (c *Connector) readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WrapError(err, errors.ErrHTTPResponse, "failed to read response body")
	}

	return bodyBytes, nil
}

// createBufferedResponse creates a new response with a buffered body that can be read again
func (c *Connector) createBufferedResponse(originalResp *http.Response, bodyBytes []byte) *http.Response {
	return &http.Response{
		Status:           originalResp.Status,
		StatusCode:       originalResp.StatusCode,
		Proto:            originalResp.Proto,
		ProtoMajor:       originalResp.ProtoMajor,
		ProtoMinor:       originalResp.ProtoMinor,
		Header:           originalResp.Header,
		Body:             io.NopCloser(bytes.NewReader(bodyBytes)),
		ContentLength:    originalResp.ContentLength,
		TransferEncoding: originalResp.TransferEncoding,
		Close:            originalResp.Close,
		Uncompressed:     originalResp.Uncompressed,
		Trailer:          originalResp.Trailer,
		Request:          originalResp.Request,
		TLS:              originalResp.TLS,
	}
}

// processResponseFromBytes processes response from pre-read body bytes
func (c *Connector) processResponseFromBytes(statusCode int, bodyBytes []byte) ([]interface{}, error) {
	if statusCode != http.StatusOK {
		return nil, errors.WrapError(fmt.Errorf("API returned status %d", statusCode), errors.ErrHTTPResponse, "unexpected status code")
	}

	// Try to detect if response is an array or object
	var responseData interface{}
	if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
		return nil, errors.WrapError(err, errors.ErrHTTPResponse, "failed to decode response JSON")
	}

	// Handle null response
	if responseData == nil {
		return []interface{}{}, nil
	}

	// Handle array at root level (like JSONPlaceholder)
	if arr, ok := responseData.([]interface{}); ok {
		return arr, nil
	}

	// Handle object with nested arrays
	if objMap, ok := responseData.(map[string]interface{}); ok {
		return c.extractItems(objMap)
	}

	return nil, fmt.Errorf("unexpected response format: %T", responseData)
}

// extractItems pulls out the slice of items from the response JSON
func (c *Connector) extractItems(responseData map[string]interface{}) ([]interface{}, error) {
	rp := c.cfg.Source.ResponseMapping.RootPath
	if rp == "" {
		if items, ok := responseData["items"].([]interface{}); ok {
			return items, nil
		}
		if data, ok := responseData["data"].([]interface{}); ok {
			return data, nil
		}
		return []interface{}{responseData}, nil
	}

	root, ok := ExtractField(responseData, rp)
	if !ok {
		return nil, fmt.Errorf("root path '%s' not found", rp)
	}

	items, ok := root.([]interface{})
	if !ok {
		return nil, fmt.Errorf("root path '%s' is not an array", rp)
	}

	return items, nil
}

// extractFields maps each raw JSON item to a map[string]interface{} based on ResponseMapping.Fields.
func (c *Connector) extractFields(items []interface{}) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for i, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("item at index %d is not a map: %v", i, item)
		}

		mapped := make(map[string]interface{})
		for _, field := range c.cfg.Source.ResponseMapping.Fields {
			value, ok := ExtractField(itemMap, field.Path)

			// Check if field is missing OR null
			if !ok || value == nil {
				if field.DefaultValue != nil {
					mapped[field.Name] = field.DefaultValue
				}
				continue
			}

			mapped[field.Name] = value
		}

		results = append(results, mapped)
	}

	return results, nil
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

func WithConnectorHTTPOptions(options ...rest.HTTPClientOption) ConnectorOption {
	return func(c *Connector) {
		var doer rest.HTTPDoer = c.client
		for _, option := range options {
			doer = option(doer)
		}

		// Handle both *http.Client and custom HTTPDoer
		if client, ok := doer.(*http.Client); ok {
			c.client = client
		} else {
			c.client = &http.Client{
				Transport: &customRoundTripper{doer: doer},
				Timeout:   c.client.Timeout,
			}
		}
	}
}

type customRoundTripper struct {
	doer rest.HTTPDoer
}

func (rt *customRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt.doer.Do(req)
}

func WithTimeout(timeout time.Duration) ConnectorOption {
	return func(c *Connector) {
		c.client.Timeout = timeout
	}
}

// WithCustomHTTPClient replaces the connector's HTTP client entirely
func WithCustomHTTPClient(client *http.Client) ConnectorOption {
	return func(c *Connector) {
		c.client = client
	}
}
