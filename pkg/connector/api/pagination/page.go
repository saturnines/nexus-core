package pagination

import (
	"fmt"
	"net/http"
	"strconv"
)

// PagePagination implements page based pagination (page=1, page=2, and so on..)

// PagePagination I have no idea how pagination actually works but this should work? change if needed.
type PagePagination struct {
	PageParam      string // Query parameter name for page number (e.g., "page")
	SizeParam      string // Query parameter name for page size (e.g., "per_page", "limit")
	PageSize       int    // Number of items per page
	TotalPagesPath string // JSON path to total pages in response (e.g., "meta.total_pages")
	StartPage      int    // Starting page number (usually 1, sometimes 0)
}

// NewPagePagination creates a new page based pagination handler
func NewPagePagination(pageParam, sizeParam string, pageSize int, totalPagesPath string) *PagePagination {
	return &PagePagination{
		PageParam:      pageParam,
		SizeParam:      sizeParam,
		PageSize:       pageSize,
		TotalPagesPath: totalPagesPath,
		StartPage:      1, // default to 1 based pagination
	}
}

// ApplyPagination adds page and size parameters to the request
func (p *PagePagination) ApplyPagination(req *http.Request, state interface{}) error {
	// find the page number
	pageNum := p.StartPage
	if state != nil {
		var ok bool
		pageNum, ok = state.(int)
		if !ok {
			return fmt.Errorf("invalid state type for page pagination: expected int, got %T", state)
		}
	}

	// Add pagination parameters to query string
	query := req.URL.Query()
	query.Set(p.PageParam, strconv.Itoa(pageNum))

	// Add page size if parameter name is specified
	if p.SizeParam != "" {
		query.Set(p.SizeParam, strconv.Itoa(p.PageSize))
	}

	req.URL.RawQuery = query.Encode()
	return nil
}

// GetNextPage determines if there's a next page based on the response
func (p *PagePagination) GetNextPage(response map[string]interface{}, currentState interface{}) (bool, interface{}, error) {
	// Get current page number
	currentPage := p.StartPage
	if currentState != nil {
		var ok bool
		currentPage, ok = currentState.(int)
		if !ok {
			return false, nil, fmt.Errorf("invalid state type: expected int, got %T", currentState)
		}
	}

	// If we have a total pages path, try to extract it
	if p.TotalPagesPath != "" {
		totalPages, err := extractIntFromPath(response, p.TotalPagesPath)
		if err == nil {
			// We found total pages, check if there's more
			if currentPage < totalPages {
				return true, currentPage + 1, nil
			}
			return false, nil, nil
		}
		// If we can't find total pages, assume there might be more
	}

	// Without total pages info, we need another strategy
	// Could check if the response has data, but for now, let the connector handle empty responses
	return true, currentPage + 1, nil
}

// extractIntFromPath is a simple helper to extract an integer from a response
// In real implementation, this might use a JSONPath library
func extractIntFromPath(data map[string]interface{}, path string) (int, error) {
	// For now, support simple paths like "total_pages" or "meta.total_pages"
	// This is a simplified implementation
	value, ok := data[path]
	if !ok {
		return 0, fmt.Errorf("path %s not found", path)
	}

	switch v := value.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

// String returns a string representation of the pagination handler
func (p *PagePagination) String() string {
	return fmt.Sprintf("PagePagination(param=%s, size=%d, start=%d)", p.PageParam, p.PageSize, p.StartPage)
}
