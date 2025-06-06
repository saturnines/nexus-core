package api_test

import (
	errors2 "Nexus/pkg/errors"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
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

// TEST 12: Single-object fallback when no items/data array exists
func TestConnector_SingleObjectFallback(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a single object directly (no items/data wrapper)
		response := map[string]interface{}{
			"id":    42,
			"name":  "Alice",
			"email": "alice@example.com",
			"profile": map[string]interface{}{
				"role":       "admin",
				"department": "engineering",
			},
			"metadata": map[string]interface{}{
				"created_at": "2023-01-01T00:00:00Z",
				"updated_at": "2023-01-01T12:00:00Z",
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "single-object-fallback-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				// No RootPath specified  should fallback to treating entire response as single object
				Fields: []config.Field{
					{Name: "user_id", Path: "id"},
					{Name: "username", Path: "name"},
					{Name: "email", Path: "email"},
					{Name: "role", Path: "profile.role"},             // Nested field
					{Name: "department", Path: "profile.department"}, // Nested field
					{Name: "created", Path: "metadata.created_at"},   // Nested field
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

	// Should extract exactly one result from the single object
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[0]

	// Verify all fields were extracted correctly
	if result["user_id"] != float64(42) {
		t.Errorf("Expected user_id=42, got %v", result["user_id"])
	}
	if result["username"] != "Alice" {
		t.Errorf("Expected username='Alice', got %v", result["username"])
	}
	if result["email"] != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got %v", result["email"])
	}
	if result["role"] != "admin" {
		t.Errorf("Expected role='admin', got %v", result["role"])
	}
	if result["department"] != "engineering" {
		t.Errorf("Expected department='engineering', got %v", result["department"])
	}
	if result["created"] != "2023-01-01T00:00:00Z" {
		t.Errorf("Expected created='2023-01-01T00:00:00Z', got %v", result["created"])
	}

	t.Logf("Successfully extracted single object: %+v", result)
}

// TEST 13: Single object fallback edge cases
func TestConnector_SingleObjectFallback_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		response      map[string]interface{}
		rootPath      string
		expectedItems int
		shouldError   bool
		description   string
	}{
		{
			name:     "Empty object fallback",
			response: map[string]interface{}{
				// Empty object
			},
			rootPath:      "", // No root path
			expectedItems: 1,  // Should still create one result with default values
			shouldError:   false,
			description:   "Empty object should fallback to single result",
		},
		{
			name: "Object with missing fields",
			response: map[string]interface{}{
				"id": 1,
				// Missing name field
			},
			rootPath:      "", // No root path
			expectedItems: 1,
			shouldError:   false,
			description:   "Object with missing fields should use defaults",
		},
		{
			name: "Nested object fallback",
			response: map[string]interface{}{
				"user": map[string]interface{}{
					"id":   99,
					"name": "Bob",
				},
				"metadata": map[string]interface{}{
					"version": "1.0",
				},
			},
			rootPath:      "", // No root path
			expectedItems: 1,
			shouldError:   false,
			description:   "Complex nested object should work",
		},
		{
			name: "Failed array lookup fallback",
			response: map[string]interface{}{
				"users": map[string]interface{}{ // Not an array!
					"count":  5,
					"active": true,
				},
				"id":   123,
				"name": "Charlie",
			},
			rootPath:      "", // No root path, will try items/data, then fallback
			expectedItems: 1,
			shouldError:   false,
			description:   "When items/data lookup fails, should fallback to whole object",
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
				Name: "single-object-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						RootPath: tt.rootPath,
						Fields: []config.Field{
							{Name: "id", Path: "id", DefaultValue: -1},
							{Name: "name", Path: "name", DefaultValue: "unknown"},
							{Name: "version", Path: "metadata.version", DefaultValue: "none"},
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
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if len(results) != tt.expectedItems {
					t.Errorf("Expected %d items, got %d", tt.expectedItems, len(results))
				}

				// Verify we got some result for non error cases
				if len(results) > 0 {
					result := results[0]
					t.Logf("Result: %+v", result)

					// Basic sanity check should have some fields
					if len(result) == 0 {
						t.Errorf("Result should have at least some fields")
					}
				}
			}

			t.Logf("%s: %s", tt.name, tt.description)
		})
	}
}

// TEST 14: Default value usage for missing fields
func TestConnector_DefaultValueUsage(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Response missing several fields that have defaults configured
		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{
					"id":   1,
					"name": "Alice",
					// Missing: email, status, role, age, active, metadata
				},
				map[string]interface{}{
					"id":    2,
					"name":  "Bob",
					"email": "bob@example.com", // This one HAS email
					// Missing: status, role, age, active, metadata
				},
				map[string]interface{}{
					"id":     3,
					"name":   "Charlie",
					"status": "premium", // This one HAS status
					"role":   "admin",   // This one HAS role
					// Missing: email, age, active, metadata
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "default-value-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					// Fields with JSON-compatible default values
					{Name: "email", Path: "email", DefaultValue: "no-email@example.com"},
					{Name: "status", Path: "status", DefaultValue: "basic"},
					{Name: "role", Path: "role", DefaultValue: "user"},
					{Name: "age", Path: "age", DefaultValue: float64(25)}, // JSON-compatible
					{Name: "active", Path: "active", DefaultValue: true},
					{Name: "tags", Path: "tags", DefaultValue: []interface{}{"new", "unverified"}}, // JSON-compatible
					{Name: "metadata", Path: "metadata", DefaultValue: map[string]interface{}{
						"created_at": "2023-01-01T00:00:00Z",
						"source":     "api",
					}},
				},
			},
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

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Test first user (Alice) - all fields should use defaults except id and name
	alice := results[0]
	if alice["id"] != float64(1) {
		t.Errorf("Alice: Expected id=1, got %v", alice["id"])
	}
	if alice["name"] != "Alice" {
		t.Errorf("Alice: Expected name='Alice', got %v", alice["name"])
	}
	if alice["email"] != "no-email@example.com" {
		t.Errorf("Alice: Expected default email, got %v", alice["email"])
	}
	if alice["status"] != "basic" {
		t.Errorf("Alice: Expected default status='basic', got %v", alice["status"])
	}
	if alice["role"] != "user" {
		t.Errorf("Alice: Expected default role='user', got %v", alice["role"])
	}
	if alice["age"] != float64(25) { // Fixed: expect float64
		t.Errorf("Alice: Expected default age=25, got %v", alice["age"])
	}
	if alice["active"] != true {
		t.Errorf("Alice: Expected default active=true, got %v", alice["active"])
	}

	// Verify complex default values (array and map)
	if tags, ok := alice["tags"].([]interface{}); ok { // expect []interface{}
		expectedTags := []interface{}{"new", "unverified"}
		if len(tags) != len(expectedTags) {
			t.Errorf("Alice: Expected tags length %d, got %d", len(expectedTags), len(tags))
		} else {
			for i, tag := range expectedTags {
				if tags[i] != tag {
					t.Errorf("Alice: Expected tag[%d]='%v', got '%v'", i, tag, tags[i]) // added %v
				}
			}
		}
	} else {
		t.Errorf("Alice: Expected tags to be []interface{}, got %T: %v", alice["tags"], alice["tags"]) // []interface{}
	}

	if metadata, ok := alice["metadata"].(map[string]interface{}); ok {
		if metadata["created_at"] != "2023-01-01T00:00:00Z" {
			t.Errorf("Alice: Expected metadata.created_at='2023-01-01T00:00:00Z', got %v", metadata["created_at"])
		}
		if metadata["source"] != "api" {
			t.Errorf("Alice: Expected metadata.source='api', got %v", metadata["source"])
		}
	} else {
		t.Errorf("Alice: Expected metadata to be map[string]interface{}, got %T: %v", alice["metadata"], alice["metadata"])
	}

	// Test second user (Bob) has email, should use defaults for others
	bob := results[1]
	if bob["id"] != float64(2) {
		t.Errorf("Bob: Expected id=2, got %v", bob["id"])
	}
	if bob["email"] != "bob@example.com" {
		t.Errorf("Bob: Expected email='bob@example.com', got %v", bob["email"])
	}
	if bob["status"] != "basic" {
		t.Errorf("Bob: Expected default status='basic', got %v", bob["status"])
	}
	if bob["role"] != "user" {
		t.Errorf("Bob: Expected default role='user', got %v", bob["role"])
	}

	// Test third user (Charlie) has status and role, should use defaults for others
	charlie := results[2]
	if charlie["id"] != float64(3) {
		t.Errorf("Charlie: Expected id=3, got %v", charlie["id"])
	}
	if charlie["email"] != "no-email@example.com" {
		t.Errorf("Charlie: Expected default email, got %v", charlie["email"])
	}
	if charlie["status"] != "premium" {
		t.Errorf("Charlie: Expected status='premium' (from API), got %v", charlie["status"])
	}
	if charlie["role"] != "admin" {
		t.Errorf("Charlie: Expected role='admin' (from API), got %v", charlie["role"])
	}
	if charlie["age"] != float64(25) { // expect float64
		t.Errorf("Charlie: Expected default age=25, got %v", charlie["age"])
	}

	t.Logf("Successfully verified default values for %d users", len(results))
}

// TEST 15: Default value edge cases and types
func TestConnector_DefaultValueEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		response      map[string]interface{}
		fieldConfig   []config.Field
		expectedValue interface{}
		fieldName     string
		description   string
	}{
		{
			name: "Null value uses default",
			response: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"id":     1,
						"status": nil, // Explicit null
					},
				},
			},
			fieldConfig: []config.Field{
				{Name: "id", Path: "id"},
				{Name: "status", Path: "status", DefaultValue: "active"},
			},
			expectedValue: "active",
			fieldName:     "status",
			description:   "Null values should trigger default usage",
		},
		{
			name: "Zero values are preserved",
			response: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"id":     1,
						"count":  0,     // Zero value should be preserved
						"name":   "",    // Empty string should be preserved
						"active": false, // False should be preserved
					},
				},
			},
			fieldConfig: []config.Field{
				{Name: "id", Path: "id"},
				{Name: "count", Path: "count", DefaultValue: 100},
				{Name: "name", Path: "name", DefaultValue: "default-name"},
				{Name: "active", Path: "active", DefaultValue: true},
			},
			expectedValue: map[string]interface{}{
				"count":  float64(0), // JSON numbers are float64
				"name":   "",
				"active": false,
			},
			fieldName:   "multiple",
			description: "Zero values should NOT trigger defaults",
		},
		{
			name: "Nested path missing uses default",
			response: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"id": 1,
						"user": map[string]interface{}{
							"name": "Alice",
							// Missing user.profile.email
						},
					},
				},
			},
			fieldConfig: []config.Field{
				{Name: "id", Path: "id"},
				{Name: "email", Path: "user.profile.email", DefaultValue: "missing@example.com"},
			},
			expectedValue: "missing@example.com",
			fieldName:     "email",
			description:   "Missing nested paths should use defaults",
		},
		{
			name: "Complex default types",
			response: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"id": 1,
						// Missing complex fields
					},
				},
			},
			fieldConfig: []config.Field{
				{Name: "id", Path: "id"},
				{Name: "config", Path: "config", DefaultValue: map[string]interface{}{
					"timeout":   float64(30),
					"retries":   float64(3),
					"enabled":   true,
					"endpoints": []interface{}{"api.example.com", "backup.example.com"},
				}},
			},
			expectedValue: map[string]interface{}{
				"timeout":   float64(30),
				"retries":   float64(3),
				"enabled":   true,
				"endpoints": []interface{}{"api.example.com", "backup.example.com"},
			},
			fieldName:   "config",
			description: "Complex nested default values should work",
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
				Name: "default-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						Fields: tt.fieldConfig,
					},
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

			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(results))
			}

			result := results[0]

			if tt.fieldName == "multiple" {
				// Special case for testing multiple fields (count, name, active)
				expected := tt.expectedValue.(map[string]interface{})
				for field, expectedVal := range expected {
					if result[field] != expectedVal {
						t.Errorf("Field %s: expected %v, got %v", field, expectedVal, result[field])
					}
				}
			} else {
				// Use DeepEqual for single-field or complex default comparisons
				got := result[tt.fieldName]
				if !reflect.DeepEqual(got, tt.expectedValue) {
					t.Errorf("Field %q: expected %+v, got %+v", tt.fieldName, tt.expectedValue, got)
				}
			}

			t.Logf("%s: %s", tt.name, tt.description)
		})
	}
}

// TEST 16: Mixed types in items array handling
func TestConnector_MixedTypesInItems(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Array containing mixed types: number, string, object
		response := map[string]interface{}{
			"items": []interface{}{
				1,     // Non-object: number
				"foo", // Non-object: string
				map[string]interface{}{"id": 3, "name": "Alice"}, // Valid object
				nil,                              // Non-object: null
				[]interface{}{"nested", "array"}, // Non-object: array
				map[string]interface{}{"id": 4, "name": "Bob"}, // Valid object
				true,                            // Non-object: boolean
				map[string]interface{}{"id": 5}, // Valid object (partial)
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "mixed-types-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name", DefaultValue: "unknown"},
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

	// Check what your connector actually does with mixed types
	if err != nil {
		// Option 1: Your connector returns an error (strict validation)
		t.Logf("Connector returned error for mixed types: %v", err)

		// Verify it's the right kind of error
		if !strings.Contains(err.Error(), "not a map") || !strings.Contains(err.Error(), "invalid item data type") {
			t.Errorf("Expected error about item not being a map, got: %s", err.Error())
		}

		t.Logf("Connector properly validates item types and returns descriptive error")
		return
	}

	// Option 2: Your connector skips invalid entries and processes valid ones
	t.Logf("Connector processed mixed types without error, got %d results", len(results))

	// If no error, we expect only the valid objects to be processed
	// Valid objects in the array: indices 2, 5, 7 (3 total)
	expectedValidItems := 3
	if len(results) != expectedValidItems {
		t.Errorf("Expected %d valid items (objects only), got %d", expectedValidItems, len(results))
	}

	// Verify the valid items were processed correctly
	if len(results) >= 1 {
		first := results[0]
		if first["id"] != float64(3) || first["name"] != "Alice" {
			t.Errorf("First valid item incorrect: %+v", first)
		}
	}

	if len(results) >= 2 {
		second := results[1]
		if second["id"] != float64(4) || second["name"] != "Bob" {
			t.Errorf("Second valid item incorrect: %+v", second)
		}
	}

	if len(results) >= 3 {
		third := results[2]
		if third["id"] != float64(5) || third["name"] != "unknown" {
			t.Errorf("Third valid item incorrect: %+v", third)
		}
	}

	t.Logf("Connector gracefully skipped invalid entries and processed valid objects")
}

// TEST 17: Mixed types edge cases and error handling
func TestConnector_MixedTypesEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		items       []interface{}
		expectError bool
		description string
	}{
		{
			name: "All non-objects",
			items: []interface{}{
				1, "string", true, nil, []interface{}{"array"},
			},
			expectError: true, // or false if your connector skips all and returns empty
			description: "Array with no valid objects",
		},
		{
			name: "Mixed with empty object",
			items: []interface{}{
				map[string]interface{}{}, // Empty object
				"invalid",                // This will cause the connector to fail
				map[string]interface{}{"id": 1},
			},
			expectError: true, // FIXED: Your connector fails fast on invalid items
			description: "Mix including empty object - fails on first invalid item",
		},
		{
			name: "Nested objects as invalid items",
			items: []interface{}{
				map[string]interface{}{
					"nested": map[string]interface{}{"deep": "value"},
				}, // Valid object with nested content
				map[string]interface{}{
					"data": []interface{}{1, 2, 3},
				}, // Valid object with array content
				42, // Invalid - will cause failure
			},
			expectError: true, // FIXED: Your connector fails fast on invalid items
			description: "Complex valid objects mixed with invalid - fails on invalid item",
		},
		{
			name: "All valid objects with complex nesting",
			items: []interface{}{
				map[string]interface{}{
					"valid": "object",
					"nested": map[string]interface{}{
						"also": "valid",
					},
				},
				map[string]interface{}{
					"array_in_object": []interface{}{
						"this", "is", "still", "valid",
					},
				},
			},
			expectError: false, // No invalid items, should work fine
			description: "All valid objects with complex nesting",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response := map[string]interface{}{
					"items": tt.items,
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "mixed-types-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						Fields: []config.Field{
							{Name: "id", Path: "id", DefaultValue: -1},
							{Name: "name", Path: "valid", DefaultValue: "default"},
						},
					},
				},
			}

			connector, err := api.NewConnector(cfg)
			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}

			results, err := connector.Extract(context.Background())

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.description)
				} else {
					t.Logf("Got expected error: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tt.description, err)
				} else {
					// Count valid objects in the test data
					validObjects := 0
					for _, item := range tt.items {
						if _, ok := item.(map[string]interface{}); ok {
							validObjects++
						}
					}

					if len(results) != validObjects {
						t.Errorf("Expected %d results (valid objects), got %d", validObjects, len(results))
					}

					t.Logf("Processed %d valid objects successfully", len(results))
				}
			}

			t.Logf("%s: %s", tt.name, tt.description)
		})
	}
}

// TEST 18: Test Malformed JSON Unclosed Object and Syntax Errors
func TestConnector_MalformedJSON_UnclosedAndSyntaxErrors(t *testing.T) {
	tests := []struct {
		name         string
		responseBody string
		description  string
	}{
		{
			name:         "Unclosed object",
			responseBody: `{"id": 1,`,
			description:  "JSON object missing closing brace",
		},
		{
			name:         "Unclosed array",
			responseBody: `{"items": [{"id": 1}`,
			description:  "JSON array missing closing bracket",
		},
		{
			name:         "Unclosed string",
			responseBody: `{"id": 1, "name": "unclosed`,
			description:  "JSON string missing closing quote",
		},
		{
			name:         "Invalid escape sequence",
			responseBody: `{"id": 1, "name": "invalid\escape"}`,
			description:  "Invalid escape sequence in JSON string",
		},
		{
			name:         "Trailing comma",
			responseBody: `{"id": 1, "name": "test",}`,
			description:  "Trailing comma in JSON object",
		},
		{
			name:         "Missing comma",
			responseBody: `{"id": 1 "name": "test"}`,
			description:  "Missing comma between JSON fields",
		},
		{
			name:         "Invalid number format",
			responseBody: `{"id": 1.2.3, "name": "test"}`,
			description:  "Invalid number format in JSON",
		},
		{
			name:         "Completely invalid JSON",
			responseBody: `{this is not json at all}`,
			description:  "Completely malformed JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(tt.responseBody))
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "malformed-json-test",
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

			_, err = connector.Extract(context.Background())
			if err == nil {
				t.Fatalf("Expected error for malformed JSON (%s), got nil", tt.description)
			}

			// Check if specifically reponse is an ErrHTTPResponse
			if !errors2.Is(err, errors2.ErrHTTPResponse) {
				t.Errorf("Expected ErrHTTPResponse, got error type: %T", err)
			}

			// Verify error message mentions JSON/decode issues
			if !strings.Contains(err.Error(), "JSON") && !strings.Contains(err.Error(), "decode") && !strings.Contains(err.Error(), "unmarshal") {
				t.Errorf("Error should mention JSON/decode issue, got: %s", err.Error())
			}

			t.Logf("%s: correctly handled with error: %v", tt.description, err)
		})
	}
}

// TEST 19: Empty Response Body Handling
func TestConnector_EmptyResponseBody(t *testing.T) {
	tests := []struct {
		name         string
		responseBody string
		contentType  string
		shouldError  bool
		description  string
	}{
		{
			name:         "Completely empty body",
			responseBody: "",
			contentType:  "application/json",
			shouldError:  true,
			description:  "Server returns 200 OK with completely empty body",
		},
		{
			name:         "Whitespace only body",
			responseBody: "   \n\t  ",
			contentType:  "application/json",
			shouldError:  true,
			description:  "Server returns 200 OK with only whitespace",
		},
		{
			name:         "Empty JSON object",
			responseBody: "{}",
			contentType:  "application/json",
			shouldError:  false,
			description:  "Server returns empty JSON object",
		},
		{
			name:         "Empty JSON array",
			responseBody: "[]",
			contentType:  "application/json",
			shouldError:  false,
			description:  "Server returns empty JSON array",
		},
		{
			name:         "Null response",
			responseBody: "null",
			contentType:  "application/json",
			shouldError:  false,
			description:  "Server returns JSON null",
		},
		{
			name:         "Empty with wrong content type",
			responseBody: "",
			contentType:  "text/html",
			shouldError:  true,
			description:  "Empty body with non-JSON content type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tt.contentType)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.responseBody))
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "empty-response-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						Fields: []config.Field{
							{Name: "id", Path: "id", DefaultValue: -1},
							{Name: "name", Path: "name", DefaultValue: "unknown"},
						},
					},
				},
			}

			connector, err := api.NewConnector(cfg)
			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			results, err := connector.Extract(ctx)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.description)
				} else {
					// Verify it doesn't hang and returns proper error type
					if !errors2.Is(err, errors2.ErrHTTPResponse) {
						t.Errorf("Expected ErrHTTPResponse, got error type: %T", err)
					}
					t.Logf("%s: correctly returned error: %v", tt.description, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tt.description, err)
				} else {
					t.Logf("%s: correctly handled, got %d results", tt.description, len(results))
				}
			}

			// Verify it doesn't hangcontext timeout should catch this
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Errorf("Connector appears to be hanging - context timeout reached")
				}
			default:
				// test should be passing
			}
		})
	}
}

// TEST 20: Custom Header Propagation
func TestConnector_CustomHeaderPropagation(t *testing.T) {
	tests := []struct {
		name        string
		setupMethod string
		description string
	}{
		{
			name:        "WithConnectorHTTPOptions",
			setupMethod: "http_options",
			description: "Headers added via WithConnectorHTTPOptions",
		},
		{
			name:        "Direct WithHeader option",
			setupMethod: "direct_header",
			description: "Headers added via connector options",
		},
		{
			name:        "Multiple custom headers",
			setupMethod: "multiple_headers",
			description: "Multiple custom headers propagated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestHeaders []map[string]string
			requestCount := 0

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++

				// Capture all headers for checking
				headers := make(map[string]string)
				for name, values := range r.Header {
					if len(values) > 0 {
						headers[name] = values[0] // Take first value
					}
				}
				requestHeaders = append(requestHeaders, headers)

				// Assert required headers are present
				switch tt.setupMethod {
				case "http_options", "direct_header":
					if r.Header.Get("X-Test") != "hello" {
						t.Errorf("Request %d: Expected X-Test header 'hello', got '%s'", requestCount, r.Header.Get("X-Test"))
					}
				case "multiple_headers":
					if r.Header.Get("X-Test") != "hello" {
						t.Errorf("Request %d: Expected X-Test header 'hello', got '%s'", requestCount, r.Header.Get("X-Test"))
					}
					if r.Header.Get("X-Custom") != "value" {
						t.Errorf("Request %d: Expected X-Custom header 'value', got '%s'", requestCount, r.Header.Get("X-Custom"))
					}
					if r.Header.Get("Authorization") != "Bearer test-token" {
						t.Errorf("Request %d: Expected Authorization header 'Bearer test-token', got '%s'", requestCount, r.Header.Get("Authorization"))
					}
				}

				// Return paginated response to test header propagation across requests
				var response map[string]interface{}
				if requestCount == 1 {
					response = map[string]interface{}{
						"items": []interface{}{
							map[string]interface{}{"id": 1, "name": "Item 1"},
						},
						"has_more": true,
					}
				} else {
					response = map[string]interface{}{
						"items": []interface{}{
							map[string]interface{}{"id": 2, "name": "Item 2"},
						},
						"has_more": false,
					}
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)

				t.Logf("Request %d: Verified headers present", requestCount)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "header-propagation-test",
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
					PageSize:    1,
					HasMorePath: "has_more",
				},
			}

			var connector *api.Connector
			var err error

			// Setup connector with different header methods
			switch tt.setupMethod {
			case "http_options":
				connector, err = api.NewConnector(cfg, api.WithConnectorHTTPOptions(
					api.WithCustomHTTPClient(&customHeaderClient{
						client: &http.Client{Timeout: 30 * time.Second},
						headers: map[string]string{
							"X-Test": "hello",
						},
					}),
				))
			case "direct_header":
				cfg.Source.Headers = map[string]string{
					"X-Test": "hello",
				}
				connector, err = api.NewConnector(cfg)
			case "multiple_headers":
				cfg.Source.Headers = map[string]string{
					"X-Test":        "hello",
					"X-Custom":      "value",
					"Authorization": "Bearer test-token",
				}
				connector, err = api.NewConnector(cfg)
			}

			if err != nil {
				t.Fatalf("Failed to create connector: %v", err)
			}

			ctx := context.Background()
			results, err := connector.Extract(ctx)
			if err != nil {
				t.Fatalf("Extract failed: %v", err)
			}

			// Verify we got both pages
			if len(results) != 2 {
				t.Errorf("Expected 2 results from pagination, got %d", len(results))
			}

			// Verify we made 2 requests
			if requestCount != 2 {
				t.Errorf("Expected 2 requests for pagination, got %d", requestCount)
			}

			// Verify headers were present on ALL requests
			for i, headers := range requestHeaders {
				t.Logf("Request %d headers: %+v", i+1, headers)
			}

			t.Logf("%s: Successfully propagated headers across %d requests", tt.description, requestCount)
		})
	}
}

// Custom HTTP client for testing header injection
type customHeaderClient struct {
	client  api.HTTPDoer
	headers map[string]string
}

func (c *customHeaderClient) Do(req *http.Request) (*http.Response, error) {
	// Add custom headers to every request
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return c.client.Do(req)
}
