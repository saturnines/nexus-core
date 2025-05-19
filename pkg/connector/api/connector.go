package api

import (
	"Nexus/pkg/config"
	"Nexus/pkg/connector/api/auth"
	"Nexus/pkg/connector/api/pagination"
	errors2 "Nexus/pkg/errors"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Connector is a main API connector that adapts to configuration
type Connector struct {
	httpClient        HTTPDoer // Changed from *http.Client to HTTPDoer
	baseURL           string
	headers           map[string]string
	config            *config.Pipeline
	authHandler       auth.Handler       // Interface from auth/auth.go
	paginationHandler pagination.Handler // Interface from pagination/pagination.go
}

// ConnectorOption defines options for the connector
type ConnectorOption func(*Connector)

// NewConnector creates a new API connector with configuration based components
func NewConnector(cfg *config.Pipeline, options ...ConnectorOption) (*Connector, error) {
	// Validate the config
	if cfg.Source.Type != config.SourceTypeREST {
		return nil, errors2.WrapError(
			fmt.Errorf("unsupported source type: %s", cfg.Source.Type),
			errors2.ErrConfiguration,
			"invalid source type",
		)
	}

	// Create simple connector
	connector := &Connector{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: cfg.Source.Endpoint,
		headers: cfg.Source.Headers,
		config:  cfg,
	}

	// Apply options
	for _, option := range options {
		option(connector)
	}

	// Set up auth if configured
	if cfg.Source.Auth != nil {
		// Create auth handler using factory
		authHandler, err := auth.CreateHandler(cfg.Source.Auth)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrConfiguration, "failed to configure authentication")
		}
		connector.authHandler = authHandler
	}

	// Set up pagination if configured
	if cfg.Pagination != nil {
		// Create pagination handler using factory
		paginationHandler, err := pagination.CreateHandler(cfg.Pagination)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrConfiguration, "failed to configure pagination")
		}
		connector.paginationHandler = paginationHandler
	}

	return connector, nil
}

func WithConnectorHTTPOptions(options ...HTTPClientOption) ConnectorOption {
	return func(c *Connector) {
		c.httpClient = ApplyHTTPClientOptions(c.httpClient, options...)
	}
}

// Extract performs data extraction with all configured components
func (c *Connector) Extract(ctx context.Context) ([]map[string]interface{}, error) {
	var allResults []map[string]interface{}
	var currentPage interface{} = nil // Start with nil for the first page

	// Main extraction loop
	for {
		// Create request
		req, err := c.createRequest(ctx, currentPage)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "failed to create request")
		}

		// Apply authentication if configured
		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				// Check for specific auth errors if defined
				var errTokenRefresh *auth.TokenRefreshError
				if errors.As(err, &errTokenRefresh) {
					return nil, errors2.WrapError(err, errors2.ErrTokenExpired, "token refresh failed")
				}
				return nil, errors2.WrapError(err, errors2.ErrAuthentication, "authentication failed")
			}
		}

		// Execute request using the HTTPDoer interface
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "request failed")
		}

		// Process response and get data
		responseData, items, err := c.processResponse(resp)
		if err != nil {
			// Error is already wrapped in processResponse
			return nil, err
		}

		// Extract fields from items
		pageResults, err := c.extractFields(items)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrExtraction, "failed to extract fields")
		}

		// Add results from this page
		allResults = append(allResults, pageResults...)

		// Check if we need to paginate
		if c.paginationHandler == nil {
			break // No pagination configured
		}

		// Get next page information
		hasNextPage, nextPage, err := c.paginationHandler.GetNextPage(responseData, currentPage)
		if err != nil {
			return nil, errors2.WrapError(err, errors2.ErrPagination, "failed to get next page")
		}

		if !hasNextPage {
			break // No more pages
		}

		// Update current page for next iteration
		currentPage = nextPage
	}

	return allResults, nil
}

// createRequest builds the HTTP request with params
func (c *Connector) createRequest(ctx context.Context, page interface{}) (*http.Request, error) {
	// Use http.NewRequestWithContext instead of http.NewRequest
	req, err := http.NewRequestWithContext(ctx, c.config.Source.Method, c.baseURL, nil)
	if err != nil {
		return nil, errors2.WrapError(err, errors2.ErrHTTPRequest, "failed to create HTTP request")
	}

	// Apply pagination parameters if configured
	if c.paginationHandler != nil && page != nil {
		if err := c.paginationHandler.ApplyPagination(req, page); err != nil {
			return nil, errors2.WrapError(err, errors2.ErrPagination, "failed to apply pagination parameters")
		}
	}

	// Apply headers
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// processResponse handles the HTTP response and extracts data
func (c *Connector) processResponse(resp *http.Response) (map[string]interface{}, []interface{}, error) {
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return nil, nil, errors2.WrapError(
			fmt.Errorf("API returned status %d", resp.StatusCode),
			errors2.ErrHTTPResponse,
			"unexpected status code",
		)
	}

	// Parse response
	var responseData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
		return nil, nil, errors2.WrapError(err, errors2.ErrHTTPResponse, "failed to decode response JSON")
	}

	// Extract items based on root path
	items, err := c.extractItems(responseData)
	if err != nil {
		// Error is already wrapped in extractItems
		return nil, nil, err
	}

	return responseData, items, nil
}

// extractItems extracts the array of items from the response data
func (c *Connector) extractItems(responseData map[string]interface{}) ([]interface{}, error) {
	// If no root path is specified, use the entire response
	if c.config.Source.ResponseMapping.RootPath == "" {
		// Check if the response is already an array
		if items, ok := responseData["items"].([]interface{}); ok {
			return items, nil
		}
		// Otherwise, treat the response itself as a single item
		return []interface{}{responseData}, nil
	}

	// Extract items using the specified root path
	root, ok := ExtractField(responseData, c.config.Source.ResponseMapping.RootPath)
	if !ok {
		return nil, errors2.WrapError(
			fmt.Errorf("root path '%s' not found", c.config.Source.ResponseMapping.RootPath),
			errors2.ErrExtraction,
			"missing root path",
		)
	}

	// Convert to array of items
	items, ok := root.([]interface{})
	if !ok {
		return nil, errors2.WrapError(
			fmt.Errorf("root path '%s' is not an array", c.config.Source.ResponseMapping.RootPath),
			errors2.ErrExtraction,
			"invalid root path data type",
		)
	}

	return items, nil
}

// extractFields extracts specific fields from items based on mapping
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

		extractedItem := make(map[string]interface{})
		for _, field := range c.config.Source.ResponseMapping.Fields {
			value, ok := ExtractField(itemMap, field.Path)
			if !ok {
				// Use default value if specified, otherwise skip
				if field.DefaultValue != nil {
					extractedItem[field.Name] = field.DefaultValue
				}
				continue
			}
			extractedItem[field.Name] = value
		}

		results = append(results, extractedItem)
	}

	return results, nil
}
