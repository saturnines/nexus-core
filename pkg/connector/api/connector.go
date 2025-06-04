package api

// This connector.go I'm not proud of, it's a bit messy.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"Nexus/pkg/config"
	"Nexus/pkg/connector/api/auth"
	"Nexus/pkg/connector/api/pagination"
	errors2 "Nexus/pkg/errors"
)

// ConnectorOption allows customizing Connector (e.g. HTTP client or pagination factory).
type ConnectorOption func(*Connector)

// Connector is the main API client. It should handle auth, pagination, request building, and field extraction.
type Connector struct {
	httpClient  HTTPDoer
	baseURL     string
	headers     map[string]string
	config      *config.Pipeline
	authHandler auth.Handler
	factory     *pagination.Factory
}

// NewConnector builds a Connector from cfg. It applies any opts before returning.
// Only REST-type sources are supported.
func NewConnector(cfg *config.Pipeline, opts ...ConnectorOption) (*Connector, error) {
	if cfg.Source.Type != config.SourceTypeREST {
		return nil, errors2.WrapError(
			fmt.Errorf("unsupported source type: %s", cfg.Source.Type),
			errors2.ErrConfiguration,
			"invalid source type",
		)
	}

	c := &Connector{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    cfg.Source.Endpoint,
		headers:    make(map[string]string),
		config:     cfg,
		factory:    pagination.DefaultFactory,
	}

	if cfg.Source.Headers != nil {
		for k, v := range cfg.Source.Headers {
			c.headers[k] = v
		}
	}

	for _, o := range opts {
		o(c)
	}

	if cfg.Source.Auth != nil {
		h, err := auth.CreateHandler(cfg.Source.Auth)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrConfiguration, "failed to configure authentication")
		}
		c.authHandler = h
	}

	return c, nil
}

// This isn't used.
// WithConnectorHTTPOptions applies custom HTTP client options.
func WithConnectorHTTPOptions(options ...HTTPClientOption) ConnectorOption {
	return func(c *Connector) {
		c.httpClient = ApplyHTTPClientOptions(c.httpClient, options...)
	}
}

// This isn't used.
// WithPaginationFactory allows supplying a custom pagination.Factory.
func WithPaginationFactory(f *pagination.Factory) ConnectorOption {
	return func(c *Connector) {
		c.factory = f
	}
}

// Extract runs the extraction loop. It builds the base request, applies auth,
// pages (if configured), decodes JSON, extracts fields, and returns a slice of maps.
func (c *Connector) Extract(ctx context.Context) ([]map[string]interface{}, error) {
	var allResults []map[string]interface{}

	// If no pagination configured: single-request path
	if c.config.Pagination == nil {
		req, err := c.createBaseRequest(ctx)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "failed to create request")
		}

		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, c.handleAuthError(err)
			}
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "request failed")
		}

		items, err := c.processResponse(resp)
		if err != nil {
			return nil, err
		}

		return c.extractFields(items)
	}

	// Pagination path
	pager, err := c.createPager(ctx)
	if err != nil {
		return nil, errors2.WrapError(err, errors2.ErrPagination, "failed to create pager")
	}

	for {
		req, err := pager.NextRequest()
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrPagination, "failed to get next request")
		}
		if req == nil {
			break
		}

		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, c.handleAuthError(err)
			}
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "request failed")
		}

		// Read body once to avoid consumption conflicts
		bodyBytes, err := c.readResponseBody(resp)
		if err != nil {
			return nil, err
		}

		// Create buffered response for pager (so it can read the body)
		bufferedResp := c.createBufferedResponse(resp, bodyBytes)

		// Update pager state using buffered response
		if err := pager.UpdateState(bufferedResp); err != nil {
			return nil, errors2.WrapError(err, errors2.ErrPagination, "failed to update pager state")
		}

		// Process the same body data for item extraction
		items, err := c.processResponseFromBytes(resp.StatusCode, bodyBytes)
		if err != nil {
			return nil, err
		}

		pageResults, err := c.extractFields(items)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrExtraction, "failed to extract fields")
		}

		allResults = append(allResults, pageResults...)
	}

	return allResults, nil
}

// createPager uses the pagination.Factory to build a Pager based on config.
func (c *Connector) createPager(ctx context.Context) (pagination.Pager, error) {
	baseReq, err := c.createBaseRequest(ctx)
	if err != nil {
		return nil, err
	}

	opts := make(map[string]interface{})
	p := c.config.Pagination
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

	case config.PaginationTypeLink:
		// no extra options
	default:
		return nil, fmt.Errorf("unsupported pagination type: %s", p.Type)
	}

	return c.factory.CreatePager(string(p.Type), c.httpClient, baseReq.Clone(baseReq.Context()), opts)
}

// createBaseRequest builds the HTTP request from config Source settings.
func (c *Connector) createBaseRequest(ctx context.Context) (*http.Request, error) {
	method := c.config.Source.Method
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL, nil)
	if err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "failed to create HTTP request")
	}

	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	if c.config.Source.QueryParams != nil {
		q := req.URL.Query()
		for k, v := range c.config.Source.QueryParams {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	return req, nil
}

// handleAuthError wraps authentication errors appropriately.
func (c *Connector) handleAuthError(err error) error {
	var tr *auth.TokenRefreshError
	if errors.As(err, &tr) {
		return errors2.WrapError(err, errors2.ErrTokenExpired, "token refresh failed")
	}
	return errors2.WrapError(err, errors2.ErrAuthentication, "authentication failed")
}

// readResponseBody safely reads and closes the response body
func (c *Connector) readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to read response body")
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

// processResponse handles response for single requests (no pagination)
func (c *Connector) processResponse(resp *http.Response) ([]interface{}, error) {
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors2.WrapError(
			fmt.Errorf("API returned status %d", resp.StatusCode),
			errors2.ErrHTTPResponse,
			"unexpected status code",
		)
	}

	var responseData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to decode response JSON")
	}

	return c.extractItems(responseData)
}

// processResponseFromBytes processes response from pre-read body bytes (for pagination)
func (c *Connector) processResponseFromBytes(statusCode int, bodyBytes []byte) ([]interface{}, error) {
	if statusCode != http.StatusOK {
		return nil, errors2.WrapError(
			fmt.Errorf("API returned status %d", statusCode),
			errors2.ErrHTTPResponse,
			"unexpected status code",
		)
	}

	var responseData map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to decode response JSON")
	}

	return c.extractItems(responseData)
}

// extractItems pulls out the slice of items from the response JSON,
// using ResponseMapping.RootPath or default "items"/"data" keys.
func (c *Connector) extractItems(responseData map[string]interface{}) ([]interface{}, error) {
	rp := c.config.Source.ResponseMapping.RootPath
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
		return nil, errors2.WrapError(
			fmt.Errorf("root path '%s' not found", rp),
			errors2.ErrExtraction,
			"missing root path",
		)
	}

	items, ok := root.([]interface{})
	if !ok {
		return nil, errors2.WrapError(
			fmt.Errorf("root path '%s' is not an array", rp),
			errors2.ErrExtraction,
			"invalid root path data type",
		)
	}

	return items, nil
}

// extractFields maps each raw JSON item to a map[string]interface{} based on ResponseMapping.Fields.
func (c *Connector) extractFields(items []interface{}) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for i, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			return nil, errors2.WrapError(
				fmt.Errorf("item at index %d is not a map: %v", i, item),
				errors2.ErrExtraction,
				"invalid item data type",
			)
		}

		mapped := make(map[string]interface{})
		for _, field := range c.config.Source.ResponseMapping.Fields {
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
