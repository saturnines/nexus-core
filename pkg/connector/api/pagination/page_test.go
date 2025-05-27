// pkg/connector/api/pagination/page_test.go
package pagination

import (
	"net/http"
	"strconv"
	"testing"
)

func TestPagePagination_ApplyPagination(t *testing.T) {
	handler := NewPagePagination("page", "per_page", 20, "total_pages")

	tests := []struct {
		name         string
		state        interface{}
		expectedPage string
		expectedSize string
	}{
		{
			name:         "First page (nil state)",
			state:        nil,
			expectedPage: "1",
			expectedSize: "20",
		},
		{
			name:         "Second page",
			state:        2,
			expectedPage: "2",
			expectedSize: "20",
		},
		{
			name:         "Fifth page",
			state:        5,
			expectedPage: "5",
			expectedSize: "20",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "https://api.example.com/items", nil)

			err := handler.ApplyPagination(req, tt.state)
			if err != nil {
				t.Fatalf("ApplyPagination failed: %v", err)
			}

			// Check query parameters
			query := req.URL.Query()
			if page := query.Get("page"); page != tt.expectedPage {
				t.Errorf("Expected page=%s, got %s", tt.expectedPage, page)
			}
			if size := query.Get("per_page"); size != tt.expectedSize {
				t.Errorf("Expected per_page=%s, got %s", tt.expectedSize, size)
			}
		})
	}
}

func TestPagePagination_GetNextPage(t *testing.T) {
	handler := NewPagePagination("page", "per_page", 20, "total_pages")

	tests := []struct {
		name         string
		response     map[string]interface{}
		currentState interface{}
		wantHasMore  bool
		wantNext     interface{}
	}{
		{
			name: "Has more pages",
			response: map[string]interface{}{
				"total_pages": 5.0, // JSON numbers are float64
				"data":        []interface{}{},
			},
			currentState: 2,
			wantHasMore:  true,
			wantNext:     3,
		},
		{
			name: "On last page",
			response: map[string]interface{}{
				"total_pages": 5.0,
				"data":        []interface{}{},
			},
			currentState: 5,
			wantHasMore:  false,
			wantNext:     nil,
		},
		{
			name: "No total_pages in response",
			response: map[string]interface{}{
				"data": []interface{}{},
			},
			currentState: 1,
			wantHasMore:  true, // Assume there might be more
			wantNext:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasMore, nextState, err := handler.GetNextPage(tt.response, tt.currentState)
			if err != nil {
				t.Fatalf("GetNextPage failed: %v", err)
			}

			if hasMore != tt.wantHasMore {
				t.Errorf("Expected hasMore=%v, got %v", tt.wantHasMore, hasMore)
			}

			if tt.wantNext == nil && nextState != nil {
				t.Errorf("Expected nil next state, got %v", nextState)
			} else if tt.wantNext != nil && nextState != tt.wantNext {
				t.Errorf("Expected next state %v, got %v", tt.wantNext, nextState)
			}
		})
	}
}

// Test pagination flow
func TestPagePagination_FullFlow(t *testing.T) {
	handler := NewPagePagination("page", "limit", 10, "total_pages")

	// Simulate paginating through 3 pages
	responses := []map[string]interface{}{
		{"total_pages": 3.0, "page": 1.0},
		{"total_pages": 3.0, "page": 2.0},
		{"total_pages": 3.0, "page": 3.0},
	}

	var state interface{} = nil

	for i, response := range responses {
		// Create request
		req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)

		// Apply pagination
		err := handler.ApplyPagination(req, state)
		if err != nil {
			t.Fatalf("Page %d: ApplyPagination failed: %v", i+1, err)
		}

		// Verify request
		expectedPage := strconv.Itoa(i + 1)
		if page := req.URL.Query().Get("page"); page != expectedPage {
			t.Errorf("Page %d: Expected page=%s, got %s", i+1, expectedPage, page)
		}

		// Get next page
		hasMore, nextState, err := handler.GetNextPage(response, state)
		if err != nil {
			t.Fatalf("Page %d: GetNextPage failed: %v", i+1, err)
		}

		if i < 2 && !hasMore {
			t.Errorf("Page %d: Expected more pages", i+1)
		}
		if i == 2 && hasMore {
			t.Errorf("Page %d: Expected no more pages", i+1)
		}

		state = nextState
	}
}
