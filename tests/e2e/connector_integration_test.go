package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"Nexus/pkg/config"
	"Nexus/pkg/connector/api"
)

// TEST 1: Nested Path Resolution
func TestConnector_NestedPaths_WillProbablyFail(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": []interface{}{
				map[string]interface{}{
					"user": map[string]interface{}{
						"profile": map[string]interface{}{
							"firstName": "John",
							"lastName":  "Doe",
							"contact": map[string]interface{}{
								"email": "john@example.com",
							},
						},
					},
					"metadata": map[string]interface{}{
						"created": "2023-01-01",
						"tags":    []string{"vip", "premium"},
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "nested-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data", // This might not work
				Fields: []config.Field{
					{Name: "first_name", Path: "user.profile.firstName"}, // Nested
					{Name: "last_name", Path: "user.profile.lastName"},   // Nested
					{Name: "email", Path: "user.profile.contact.email"},  // Deep nested
					{Name: "created_date", Path: "metadata.created"},     // Different branch
					{Name: "first_tag", Path: "metadata.tags[0]"},        // Array access
				},
			},
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

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[0]
	t.Logf("Result: %+v", result)

	if result["first_name"] != "John" {
		t.Errorf("Expected first_name='John', got %v", result["first_name"])
	}
	if result["email"] != "john@example.com" {
		t.Errorf("Expected email='john@example.com', got %v", result["email"])
	}
}

// TEST 2: Malformed JSON
func TestConnector_MalformedJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items": [{"id": 1, "name": "John"} // Missing closing bracket and quote`))
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "malformed-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx := context.Background()
	_, err = connector.Extract(ctx)
	if err == nil {
		t.Fatal("Expected error for malformed JSON, got nil")
	}

	// Check that the error message is helpful
	if !strings.Contains(err.Error(), "JSON") && !strings.Contains(err.Error(), "decode") {
		t.Errorf("Error should mention JSON parsing issue, got: %s", err.Error())
	}
}

// TEST 3: Empty Response
func TestConnector_EmptyResponse(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items": []}`)) // Empty array
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "empty-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
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

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty array, got %d", len(results))
	}
}

// TEST 4: Missing Fields with Defaults
func TestConnector_MissingFieldsWithDefaults(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{
					"id":   1,
					"name": "John",
					// Missing "email" field
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "defaults-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					{Name: "email", Path: "email", DefaultValue: "no-email@example.com"},
					{Name: "status", Path: "status", DefaultValue: "active"},
				},
			},
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

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result["email"] != "no-email@example.com" {
		t.Errorf("Expected default email, got %v", result["email"])
	}
	if result["status"] != "active" {
		t.Errorf("Expected default status, got %v", result["status"])
	}
}

// TEST 5: Timeout Simulation
func TestConnector_SlowResponse(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // Simulate slow API
		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"id": 1, "name": "Slow Response"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "timeout-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
	}

	// Create connector with short timeout
	connector, err := api.NewConnector(cfg, api.WithConnectorHTTPOptions(
		api.WithTimeout(1*time.Second), // 1 second timeout, but server takes 2 seconds
	))
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx := context.Background()
	_, err = connector.Extract(ctx)
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "context deadline") {
		t.Errorf("Expected timeout error, got: %s", err.Error())
	}
}

// TEST 6: Different Response Structures
func TestConnector_DifferentResponseStructure(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Response is a single object, not an arr
		response := map[string]interface{}{
			"id":   42,
			"name": "Single Object Response",
			"meta": map[string]interface{}{
				"total": 1,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "single-object-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				// No RootPath (should treat response as single item)
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
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

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result["id"] != float64(42) {
		t.Errorf("Expected id=42, got %v", result["id"])
	}
}

// TEST 7: Pagination Test Page based
func TestConnector_PagePagination(t *testing.T) {
	pageRequests := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageRequests++
		page := r.URL.Query().Get("page")
		size := r.URL.Query().Get("size")

		t.Logf("Page request #%d: page=%s, size=%s", pageRequests, page, size)

		var response map[string]interface{}

		switch page {
		case "", "1":
			response = map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "name": "User 1"},
					map[string]interface{}{"id": 2, "name": "User 2"},
				},
				"has_more": true,
			}
		case "2":
			response = map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 3, "name": "User 3"},
				},
				"has_more": false,
			}
		default:
			response = map[string]interface{}{
				"items":    []interface{}{},
				"has_more": false,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "pagination-test",
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
			Type:        config.PaginationTypePage,
			PageParam:   "page",
			SizeParam:   "size",
			PageSize:    2,
			HasMorePath: "has_more",
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

	// Should get 3 total results from 2 pages
	if len(results) != 3 {
		t.Errorf("Expected 3 total results from pagination, got %d", len(results))
	}

	// Should have made 2 requests
	if pageRequests != 2 {
		t.Errorf("Expected 2 page requests, got %d", pageRequests)
	}

	t.Logf("Final results: %+v", results)
}

// Helper to generate a paginated response with a fixed total_pages and optional last‐page count.
func makePageHandler(t *testing.T, totalPages, pageSize, itemsLastPage int, log *[]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pageParam := r.URL.Query().Get("page")
		sizeParam := r.URL.Query().Get("page_size")
		*log = append(*log, fmt.Sprintf("page=%s&page_size=%s", pageParam, sizeParam))

		pageNum := 1
		if pageParam != "" {
			if p, err := strconv.Atoi(pageParam); err == nil && p > 0 {
				pageNum = p
			}
		}

		if pageNum > totalPages {
			t.Errorf("Received unexpected request for page %d (total_pages=%d)", pageNum, totalPages)
			// Return empty payload so connector can finish gracefully
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"items": []interface{}{},
				"meta": map[string]interface{}{
					"current_page": pageNum,
					"total_pages":  totalPages,
					"per_page":     pageSize,
				},
			})
			return
		}

		// Determine how many items to return on this page
		currentCount := pageSize
		if pageNum == totalPages {
			currentCount = itemsLastPage
		}
		startID := (pageNum-1)*pageSize + 1

		items := make([]interface{}, 0, currentCount)
		for i := 0; i < currentCount; i++ {
			items = append(items, map[string]interface{}{
				"id":   startID + i,
				"name": fmt.Sprintf("User %d", startID+i),
				"page": pageNum,
			})
		}

		response := map[string]interface{}{
			"items": items,
			"meta": map[string]interface{}{
				"current_page": pageNum,
				"total_pages":  totalPages,
				"per_page":     pageSize,
				"total_count":  (totalPages-1)*pageSize + itemsLastPage,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Failed to encode JSON: %v", err)
		}
	}
}

// TEST 8: Page-based pagination driven by meta.total_pages
func TestConnector_PagePagination_TotalPages(t *testing.T) {
	var requestLog []string
	totalPages := 4
	itemsPerPage := 3
	// Last page has exactly itemsPerPage items (a “full” final page)
	itemsLastPage := itemsPerPage

	mockServer := httptest.NewServer(makePageHandler(t, totalPages, itemsPerPage, itemsLastPage, &requestLog))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "page-total-pages-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					{Name: "page", Path: "page"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:           config.PaginationTypePage,
			PageParam:      "page",
			SizeParam:      "page_size",
			PageSize:       itemsPerPage,
			TotalPagesPath: "meta.total_pages",
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

	// Expect 4 pages × 3 items each = 12
	expectedItems := totalPages * itemsPerPage
	if got := len(results); got != expectedItems {
		t.Errorf("Expected %d total items, got %d", expectedItems, got)
	}

	// Verify all four pages were requested
	expectedRequests := []string{
		"page=1&page_size=3",
		"page=2&page_size=3",
		"page=3&page_size=3",
		"page=4&page_size=3",
	}
	if len(requestLog) != len(expectedRequests) {
		t.Errorf("Expected %d requests, got %d: %v", len(expectedRequests), len(requestLog), requestLog)
	}
	for i, exp := range expectedRequests {
		if i < len(requestLog) && requestLog[i] != exp {
			t.Errorf("Request %d: expected %s, got %s", i+1, exp, requestLog[i])
		}
	}

	// Spot‐check some items for correctness
	if len(results) > 0 {
		first := results[0]
		if first["id"] != float64(1) || first["name"] != "User 1" || first["page"] != float64(1) {
			t.Errorf("First item incorrect: %+v", first)
		}
	}
	if len(results) >= 7 {
		seventh := results[6] // start of page 3
		if seventh["id"] != float64(7) || seventh["name"] != "User 7" || seventh["page"] != float64(3) {
			t.Errorf("Seventh item (page 3 start) incorrect: %+v", seventh)
		}
	}
	if len(results) == expectedItems {
		last := results[expectedItems-1]
		if last["id"] != float64(expectedItems) || last["name"] != fmt.Sprintf("User %d", expectedItems) {
			t.Errorf("Last item incorrect: %+v", last)
		}
	}

	t.Logf("Paginated through %d pages, collected %d items", totalPages, len(results))
}

// TEST 9: Page pagination with meta.total_pages edge cases
func TestConnector_PagePagination_TotalPages_EdgeCases(t *testing.T) {
	type tc struct {
		name             string
		totalPages       int
		itemsLastPage    int
		expectedItems    int
		expectedRequests int
		description      string
	}
	tests := []tc{
		{
			name:             "Single page",
			totalPages:       1,
			itemsLastPage:    5,
			expectedItems:    5,
			expectedRequests: 1,
			description:      "Dataset fits in one page",
		},
		{
			name:             "Empty last page",
			totalPages:       3,
			itemsLastPage:    0,
			expectedItems:    2 + 2 + 0,
			expectedRequests: 3,
			description:      "Last page returns empty array",
		},
		{
			name:             "Partial last page",
			totalPages:       3,
			itemsLastPage:    1,
			expectedItems:    2 + 2 + 1,
			expectedRequests: 3,
			description:      "Last page has fewer items than page size",
		},
		{
			name:             "Overflow last page",
			totalPages:       2,
			itemsLastPage:    5,     // exceeds pageSize
			expectedItems:    2 + 5, // connector will take whatever comes back
			expectedRequests: 2,
			description:      "Last page returns more items than page size",
		},
	}

	pageSize := 2
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestLog []string
			mockServer := httptest.NewServer(
				makePageHandler(t, tt.totalPages, pageSize, tt.itemsLastPage, &requestLog),
			)
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "page-edge-case-test",
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
					Type:           config.PaginationTypePage,
					PageParam:      "page",
					SizeParam:      "page_size",
					PageSize:       pageSize,
					TotalPagesPath: "meta.total_pages",
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

			if got := len(results); got != tt.expectedItems {
				t.Errorf("Expected %d items, got %d", tt.expectedItems, got)
			}
			if gotReq := len(requestLog); gotReq != tt.expectedRequests {
				t.Errorf("Expected %d requests, got %d: %v", tt.expectedRequests, gotReq, requestLog)
			}

			// Spot‐check boundaries for each page if any items exist
			if len(results) > 0 && tt.expectedItems > 0 {
				first := results[0]
				if first["id"] != float64(1) || first["name"] != "User 1" {
					t.Errorf("Page 1, first item wrong: %+v", first)
				}
			}
			if tt.totalPages > 1 && len(results) >= pageSize+1 {
				secondPageFirst := results[pageSize]
				if secondPageFirst["id"] != float64(pageSize+1) {
					t.Errorf("Page 2, first item ID expected %d, got %v", pageSize+1, secondPageFirst["id"])
				}
			}

			t.Logf("%s: %d requests, %d items", tt.name, len(requestLog), len(results))
		})
	}
}

// TEST 10: Missing root path with nested data structure
func TestConnector_MissingRootPath_NestedData(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Response with nested structure: data.users array
		response := map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{
						"id":       1,
						"username": "alice",
						"profile": map[string]interface{}{
							"email": "alice@example.com",
							"role":  "admin",
						},
					},
					map[string]interface{}{
						"id":       2,
						"username": "bob",
						"profile": map[string]interface{}{
							"email": "bob@example.com",
							"role":  "user",
						},
					},
					map[string]interface{}{
						"id":       3,
						"username": "charlie",
						"profile": map[string]interface{}{
							"email": "charlie@example.com",
							"role":  "user",
						},
					},
				},
				"total_count": 3,
				"page_info": map[string]interface{}{
					"current_page": 1,
					"total_pages":  1,
				},
			},
			"timestamp": "2023-01-01T00:00:00Z",
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "nested-root-path-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.users", // Deep nested path to users array
				Fields: []config.Field{
					{Name: "user_id", Path: "id"},
					{Name: "username", Path: "username"},
					{Name: "email", Path: "profile.email"}, // Nested field extraction
					{Name: "role", Path: "profile.role"},   // Nested field extraction
				},
			},
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

	// Should extract 3 users from data.users array
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Verify first user
	firstUser := results[0]
	if firstUser["user_id"] != float64(1) {
		t.Errorf("Expected user_id=1, got %v", firstUser["user_id"])
	}
	if firstUser["username"] != "alice" {
		t.Errorf("Expected username='alice', got %v", firstUser["username"])
	}
	if firstUser["email"] != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got %v", firstUser["email"])
	}
	if firstUser["role"] != "admin" {
		t.Errorf("Expected role='admin', got %v", firstUser["role"])
	}

	// Verify second user
	secondUser := results[1]
	if secondUser["user_id"] != float64(2) {
		t.Errorf("Expected user_id=2, got %v", secondUser["user_id"])
	}
	if secondUser["username"] != "bob" {
		t.Errorf("Expected username='bob', got %v", secondUser["username"])
	}
	if secondUser["email"] != "bob@example.com" {
		t.Errorf("Expected email='bob@example.com', got %v", secondUser["email"])
	}
	if secondUser["role"] != "user" {
		t.Errorf("Expected role='user', got %v", secondUser["role"])
	}

	// Verify third user
	thirdUser := results[2]
	if thirdUser["user_id"] != float64(3) {
		t.Errorf("Expected user_id=3, got %v", thirdUser["user_id"])
	}
	if thirdUser["username"] != "charlie" {
		t.Errorf("Expected username='charlie', got %v", thirdUser["username"])
	}
	if thirdUser["email"] != "charlie@example.com" {
		t.Errorf("Expected email='charlie@example.com', got %v", thirdUser["email"])
	}
	if thirdUser["role"] != "user" {
		t.Errorf("Expected role='user', got %v", thirdUser["role"])
	}

	t.Logf("Successfully extracted %d users from nested root path 'data.users'", len(results))
}

// TEST 11: Edge cases for root path handling
func TestConnector_RootPath_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		rootPath      string
		response      map[string]interface{}
		expectedItems int
		shouldError   bool
		errorContains string
	}{
		{
			name:     "Valid deeply nested path",
			rootPath: "response.data.items.list",
			response: map[string]interface{}{
				"response": map[string]interface{}{
					"data": map[string]interface{}{
						"items": map[string]interface{}{
							"list": []interface{}{
								map[string]interface{}{"id": 1, "name": "Item 1"},
								map[string]interface{}{"id": 2, "name": "Item 2"},
							},
						},
					},
				},
			},
			expectedItems: 2,
			shouldError:   false,
		},
		{
			name:     "Missing root path",
			rootPath: "data.nonexistent",
			response: map[string]interface{}{
				"data": map[string]interface{}{
					"items": []interface{}{
						map[string]interface{}{"id": 1, "name": "Item 1"},
					},
				},
			},
			expectedItems: 0,
			shouldError:   true,
			errorContains: "root path 'data.nonexistent' not found",
		},
		{
			name:     "Root path points to non-array",
			rootPath: "data.metadata",
			response: map[string]interface{}{
				"data": map[string]interface{}{
					"metadata": map[string]interface{}{
						"total": 5,
						"page":  1,
					},
				},
			},
			expectedItems: 0,
			shouldError:   true,
			errorContains: "root path 'data.metadata' is not an array",
		},
		{
			name:     "Empty array at root path",
			rootPath: "data.users",
			response: map[string]interface{}{
				"data": map[string]interface{}{
					"users": []interface{}{}, // Empty array
				},
			},
			expectedItems: 0,
			shouldError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(tt.response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "root-path-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						RootPath: tt.rootPath,
						Fields: []config.Field{
							{Name: "id", Path: "id"},
							{Name: "name", Path: "name"},
						},
					},
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
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got: %s", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if len(results) != tt.expectedItems {
					t.Errorf("Expected %d items, got %d", tt.expectedItems, len(results))
				}
			}

			t.Logf("%s: %s", tt.name, func() string {
				if tt.shouldError {
					return "correctly handled error case"
				}
				return fmt.Sprintf("extracted %d items", len(results))
			}())
		})
	}
}
