package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/saturnines/nexus-core/pkg/auth"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors"
	"github.com/saturnines/nexus-core/pkg/pagination"
	"github.com/saturnines/nexus-core/pkg/transport/rest"
)

// Connector orchestrates requests, pagination and handling.
type Connector struct {
	builder RequestBuilder
	pager   pagination.Pager
	client  *http.Client
	cfg     *config.Pipeline
}

// NewConnector picks a Builder and a Pager based on cfg.
func NewConnector(cfg *config.Pipeline) (*Connector, error) {
	h, err := auth.CreateHandler(cfg.Source.Auth)
	if err != nil {
		return nil, errors.Wrap(err, "auth handler")
	}

	var builder RequestBuilder
	var pager pagination.Pager

	switch cfg.Source.Type {
	case config.SourceTypeREST:
		builder = rest.NewBuilder(
			cfg.Source.Endpoint,
			cfg.Source.Method,
			cfg.Source.Headers,
			cfg.Source.QueryParams,
			h,
		)
		pager = pagination.DefaultFactory.New(cfg.Source.Paging.Type)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Source.Type)
	}

	return &Connector{
		builder: builder,
		pager:   pager,
		client:  &http.Client{Timeout: 30 * time.Second},
		cfg:     cfg,
	}, nil
}

// Extract runs the loop: build → send → handle → page.
func (c *Connector) Extract(ctx context.Context) ([]map[string]interface{}, error) {
	var all []map[string]interface{}

	req, err := c.builder.Build(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	for {
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, errors.Wrap(err, "http do")
		}

		batch, err := c.handleResponse(resp)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)

		if !c.pager.HasNext() {
			break
		}
		if err := c.pager.Update(resp); err != nil {
			return nil, errors.Wrap(err, "paginate")
		}

		req, err = c.builder.Build(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "build next request")
		}
	}

	return all, nil
}

// createPager uses the pagination.Factory to build a Pager based on config.
func (c *Connector) createPager(ctx context.Context) (pagination.Pager, error) {
	baseReq, err := c.createBaseRequest(ctx)
	if err != nil {
		return nil, err
	}
	// This is so ugly
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

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to read response body")
	}

	return c.processResponseFromBytes(resp.StatusCode, bodyBytes)
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

	// Try to detect if response is an array or object
	var responseData interface{}
	if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to decode response JSON")
	}

	// Handle null response
	if responseData == nil {
		return []interface{}{}, nil
	}

	// Handle array at root level (like JSONPlaceholder)
	if arr, ok := responseData.([]interface{}); ok {
		return arr, nil
	}

	// Handle object with nested arrays (your existing logic)
	if objMap, ok := responseData.(map[string]interface{}); ok {
		return c.extractItems(objMap)
	}

	return nil, errors2.WrapError(
		fmt.Errorf("unexpected response format: %T", responseData),
		errors2.ErrHTTPResponse,
		"invalid response structure",
	)
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
