// pkg/connector/api/connector.go

package api

import (
	"Nexus/pkg/config"
	"Nexus/pkg/connector/api/auth"
	"Nexus/pkg/connector/api/pagination"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Connector is a main API connector that adapts to configuration
type Connector struct {
	client            *http.Client
	baseURL           string
	headers           map[string]string
	config            *config.Pipeline
	authHandler       auth.Handler       // Interface from auth/auth.go
	paginationHandler pagination.Handler // Interface from pagination/pagination.go
}

// NewConnector creates a new API connector with configuration based components
func NewConnector(cfg *config.Pipeline) (*Connector, error) {
	// Validate the config
	if cfg.Source.Type != config.SourceTypeREST {
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Source.Type)
	}

	// Create simple connector
	connector := &Connector{
		client:  &http.Client{},
		baseURL: cfg.Source.Endpoint,
		headers: cfg.Source.Headers,
		config:  cfg,
	}

	// Set up auth if configured
	if cfg.Source.Auth != nil {
		// Create auth handler using factory
		authHandler, err := auth.CreateHandler(cfg.Source.Auth)
		if err != nil {
			return nil, fmt.Errorf("failed to configure authentication: %w", err)
		}
		connector.authHandler = authHandler
	}

	// Set up pagination if configured
	if cfg.Pagination != nil {
		// Create pagination handler using factory
		paginationHandler, err := pagination.CreateHandler(cfg.Pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to configure pagination: %w", err)
		}
		connector.paginationHandler = paginationHandler
	}

	return connector, nil
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
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Apply authentication if configured
		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, fmt.Errorf("authentication error: %w", err)
			}
		}

		// Execute request
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		// Process response and get data
		responseData, items, err := c.processResponse(resp)
		if err != nil {
			return nil, err
		}

		// Extract fields from items
		pageResults, err := c.extractFields(items)
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("pagination error: %w", err)
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
	// Create base request
	req, err := http.NewRequestWithContext(ctx, c.config.Source.Method, c.baseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Apply pagination parameters if configured
	if c.paginationHandler != nil && page != nil {
		if err := c.paginationHandler.ApplyPagination(req, page); err != nil {
			return nil, fmt.Errorf("failed to apply pagination: %w", err)
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
		return nil, nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	// Parse response
	var responseData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
		return nil, nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Extract items based on root path
	items, err := c.extractItems(responseData)
	if err != nil {
		return nil, nil, err
	}

	return responseData, items, nil
}

// extractFields extracts specific fields from items based on mapping
func (c *Connector) extractFields(items []interface{}) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for _, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("item is not a map: %v", item)
		}

		extractedItem := make(map[string]interface{})
		for _, field := range c.config.Source.ResponseMapping.Fields {
			value, err := c.extractField(itemMap, field.Path)
			if err != nil {
				return nil, fmt.Errorf("failed to extract field %s: %w", field.Name, err)
			}
			extractedItem[field.Name] = value
		}

		results = append(results, extractedItem)
	}

	return results, nil
}

// Keep your existing extractItems and extractField methods...

// WithTimeout sets the request timeout
func (c *Connector) WithTimeout(timeout time.Duration) *Connector {
	c.client.Timeout = timeout
	return c
}

// WithHTTPClient sets a custom HTTP client
func (c *Connector) WithHTTPClient(client *http.Client) *Connector {
	c.client = client
	return c
}
