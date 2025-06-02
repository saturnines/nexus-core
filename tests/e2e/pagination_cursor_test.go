package api

import (
	"Nexus/pkg/config"
	"Nexus/pkg/connector/api"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// CURSOR PAGINATION TESTS

// TestConnector_CursorPagination_Complete tests cursor based pagination
func TestConnector_CursorPagination_Complete(t *testing.T) {
	var requestLog []string
	totalPages := 4
	cursors := []string{"", "cursor_page_2", "cursor_page_3", "cursor_page_4"}
	itemsPerPage := []int{3, 2, 3, 1} // Items for each page

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		requestLog = append(requestLog, fmt.Sprintf("cursor=%s", cursor))

		// Find which page we're on
		pageNum := 0
		for i, c := range cursors {
			if c == cursor {
				pageNum = i + 1
				break
			}
		}

		if pageNum == 0 {
			// Unknown cursor
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "Invalid cursor"}`))
			return
		}

		// Determine next cursor
		var nextCursor string
		if pageNum < totalPages {
			nextCursor = cursors[pageNum] // Next cursor in sequence
		}

		// Generate items for this page
		var items []interface{}
		itemCount := itemsPerPage[pageNum-1]
		startID := 1
		for i := 0; i < pageNum-1; i++ {
			startID += itemsPerPage[i]
		}

		for i := 0; i < itemCount; i++ {
			itemID := startID + i
			items = append(items, map[string]interface{}{
				"id":   itemID,
				"name": fmt.Sprintf("Item %d", itemID),
				"page": pageNum,
			})
		}

		response := map[string]interface{}{
			"data": items,
			"pagination": map[string]interface{}{
				"next_cursor": nextCursor,
				"page":        pageNum,
				"has_more":    nextCursor != "",
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		t.Logf("Served page %d (cursor='%s'): %d items, next_cursor='%s'", pageNum, cursor, len(items), nextCursor)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "cursor-pagination-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					{Name: "page", Path: "page"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "pagination.next_cursor",
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx := context.Background()
	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Calculate expected total items
	expectedItems := 0
	for _, count := range itemsPerPage {
		expectedItems += count
	}

	if len(results) != expectedItems {
		t.Errorf("Expected %d total results, got %d", expectedItems, len(results))
	}

	// Verify request sequence
	expectedRequests := []string{
		"cursor=",              // First request (no cursor)
		"cursor=cursor_page_2", // Second request
		"cursor=cursor_page_3", // Third request
		"cursor=cursor_page_4", // Fourth request
	}

	if len(requestLog) != len(expectedRequests) {
		t.Errorf("Expected %d requests, got %d: %v", len(expectedRequests), len(requestLog), requestLog)
	}

	for i, expected := range expectedRequests {
		if i < len(requestLog) && requestLog[i] != expected {
			t.Errorf("Request %d: expected %s, got %s", i+1, expected, requestLog[i])
		}
	}

	// double check data integrity
	if len(results) > 0 {
		first := results[0]
		if first["id"] != float64(1) || first["name"] != "Item 1" {
			t.Errorf("First item incorrect: %+v", first)
		}
	}

	if len(results) == expectedItems {
		last := results[expectedItems-1]
		if last["id"] != float64(expectedItems) || last["name"] != fmt.Sprintf("Item %d", expectedItems) {
			t.Errorf("Last item incorrect: %+v", last)
		}
	}

	t.Logf("Successfully cursor-paginated through %d requests, collected %d items", len(requestLog), len(results))
}

// TestConnector_CursorPagination_NestedPath tests nested cursor paths
func TestConnector_CursorPagination_NestedPath(t *testing.T) {
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		cursor := r.URL.Query().Get("cursor")

		var nextCursor string
		var items []interface{}

		if cursor == "" {
			// First page
			nextCursor = "deep_nested_cursor_123"
			items = []interface{}{
				map[string]interface{}{"id": 1, "data": "First page"},
			}
		} else if cursor == "deep_nested_cursor_123" {
			// Second page  no next cursor so end here
			nextCursor = ""
			items = []interface{}{
				map[string]interface{}{"id": 2, "data": "Second page"},
			}
		}

		response := map[string]interface{}{
			"results": items,
			"meta": map[string]interface{}{
				"pagination": map[string]interface{}{
					"cursors": map[string]interface{}{
						"next": nextCursor, // Deeply nested path
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "nested-cursor-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "results",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "data", Path: "data"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "meta.pagination.cursors.next", // Deep nested path
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Successfully handled deeply nested cursor path")
}

// TestConnector_CursorPagination_SpecialCharacters tests cursors with special characters
func TestConnector_CursorPagination_SpecialCharacters(t *testing.T) {
	specialCursor := "cursor+with%20spaces&special=chars"
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		cursor := r.URL.Query().Get("cursor")

		var nextCursor string
		var items []interface{}

		if cursor == "" {
			// First page with special character cursor
			nextCursor = specialCursor
			items = []interface{}{
				map[string]interface{}{"id": 1, "name": "Item 1"},
			}
		} else if cursor == specialCursor {
			// Second page check special characters were preserved
			nextCursor = ""
			items = []interface{}{
				map[string]interface{}{"id": 2, "name": "Item 2"},
			}
			t.Logf("Received special cursor correctly: '%s'", cursor)
		}

		response := map[string]interface{}{
			"items":       items,
			"next_cursor": nextCursor,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "special-cursor-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "next_cursor",
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Successfully handled cursor with special characters")
}

// TestConnector_CursorPagination_EdgeCases tests various edge cases
func TestConnector_CursorPagination_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		setupServer   func() *httptest.Server
		expectedItems int
		expectedReqs  int
		shouldError   bool
	}{
		{
			name: "Single page - no cursor returned",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					response := map[string]interface{}{
						"items": []interface{}{
							map[string]interface{}{"id": 1},
						},
						// No next_cursor field should stop
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(response)
				}))
			},
			expectedItems: 1,
			expectedReqs:  1,
			shouldError:   false,
		},
		{
			name: "Null cursor value",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					response := map[string]interface{}{
						"items": []interface{}{
							map[string]interface{}{"id": 1},
						},
						"next_cursor": nil, // Explicit null
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(response)
				}))
			},
			expectedItems: 1,
			expectedReqs:  1,
			shouldError:   false,
		},
		{
			name: "Empty string cursor",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					response := map[string]interface{}{
						"items": []interface{}{
							map[string]interface{}{"id": 1},
						},
						"next_cursor": "", // Empty string
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(response)
				}))
			},
			expectedItems: 1,
			expectedReqs:  1,
			shouldError:   false,
		},
		{
			name: "Server error with cursor",
			setupServer: func() *httptest.Server {
				reqCount := 0
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reqCount++
					if reqCount == 1 {
						// First request succeeds
						response := map[string]interface{}{
							"items": []interface{}{
								map[string]interface{}{"id": 1},
							},
							"next_cursor": "error_cursor",
						}
						w.Header().Set("Content-Type", "application/json")
						json.NewEncoder(w).Encode(response)
					} else {
						// Second request fails
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte(`{"error": "Server error"}`))
					}
				}))
			},
			expectedItems: 0, // No items due to error
			expectedReqs:  2,
			shouldError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := tt.setupServer()
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "cursor-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						Fields: []config.Field{
							{Name: "id", Path: "id"},
						},
					},
				},
				Pagination: &config.Pagination{
					Type:        config.PaginationTypeCursor,
					CursorParam: "cursor",
					CursorPath:  "next_cursor",
				},
			}

			connector, err := api.NewConnector(cfg)
			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}

			results, err := connector.Extract(context.Background())

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Extract failed: %v", err)
				}

				if len(results) != tt.expectedItems {
					t.Errorf("Expected %d items, got %d", tt.expectedItems, len(results))
				}
			}

			t.Logf("%s completed", tt.name)
		})
	}
}

// TestConnector_CursorPagination_EmptyDataset tests completely empty response
func TestConnector_CursorPagination_EmptyDataset(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"items": []interface{}{}, // Empty array
			// No next_cursor
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "cursor-empty-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "next_cursor",
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty dataset, got %d", len(results))
	}

	t.Logf("Correctly handled empty cursor-paginated dataset")
}
