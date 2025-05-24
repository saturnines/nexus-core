package pagination

import (
	"net/http"
)

// Handler defines the interface for pagination handlers
type Handler interface {
	// ApplyPagination modifies the request to include pagination parameters
	// For the first page state will be None
	ApplyPagination(req *http.Request, state interface{}) error

	// GetNextPage analyzes the response to determine if there are more pages
	// Returns: hasMore, nextState, error
	GetNextPage(response map[string]interface{}, currentState interface{}) (bool, interface{}, error)
}
