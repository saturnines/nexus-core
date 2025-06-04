package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"Nexus/pkg/config"
	"Nexus/pkg/connector/api"
)

// TestConnector_PagePagination_Complete tests robust page based pagination
func TestConnector_PagePagination_Complete(t *testing.T) {
	var requestLog []string
	totalItems := 25
	pageSize := 10
	totalPages := 3 // 10 + 10 + 5 = 25 items

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Log the request
		page := r.URL.Query().Get("page")
		size := r.URL.Query().Get("size")
		requestLog = append(requestLog, fmt.Sprintf("page=%s&size=%s", page, size))

		pageNum, err := strconv.Atoi(page)
		if err != nil || pageNum < 1 {
			pageNum = 1
		}

		sizeNum, err := strconv.Atoi(size)
		if err != nil || sizeNum < 1 {
			sizeNum = pageSize
		}

		// Calculate items for this page
		startIdx := (pageNum - 1) * sizeNum
		endIdx := startIdx + sizeNum
		if endIdx > totalItems {
			endIdx = totalItems
		}

		var items []interface{}
		for i := startIdx; i < endIdx; i++ {
			items = append(items, map[string]interface{}{
				"id":   i + 1,
				"name": fmt.Sprintf("User %d", i+1),
				"page": pageNum,
			})
		}

		response := map[string]interface{}{
			"items": items,
			"meta": map[string]interface{}{
				"total_pages":    totalPages,
				"current_page":   pageNum,
				"total_items":    totalItems,
				"items_per_page": sizeNum,
			},
			"has_more": pageNum < totalPages,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		t.Logf("Served page %d: %d items (IDs %d-%d)", pageNum, len(items), startIdx+1, endIdx)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "page-pagination-test",
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
			Type:        config.PaginationTypePage,
			PageParam:   "page",
			SizeParam:   "size",
			PageSize:    pageSize,
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

	// Verify total results
	if len(results) != totalItems {
		t.Errorf("Expected %d total results, got %d", totalItems, len(results))
	}

	// Verify request sequence
	expectedRequests := []string{
		"page=1&size=10",
		"page=2&size=10",
		"page=3&size=10",
	}

	if len(requestLog) != len(expectedRequests) {
		t.Errorf("Expected %d requests, got %d: %v", len(expectedRequests), len(requestLog), requestLog)
	}

	for i, expected := range expectedRequests {
		if i < len(requestLog) && requestLog[i] != expected {
			t.Errorf("Request %d: expected %s, got %s", i+1, expected, requestLog[i])
		}
	}

	// Verify data integrity check first middle last items
	if len(results) > 0 {
		first := results[0]
		if first["id"] != float64(1) || first["name"] != "User 1" {
			t.Errorf("First item incorrect: %+v", first)
		}
	}

	if len(results) >= 15 {
		middle := results[14] // 15th item
		if middle["id"] != float64(15) || middle["name"] != "User 15" {
			t.Errorf("Middle item incorrect: %+v", middle)
		}
	}

	if len(results) == totalItems {
		last := results[totalItems-1]
		if last["id"] != float64(totalItems) || last["name"] != fmt.Sprintf("User %d", totalItems) {
			t.Errorf("Last item incorrect: %+v", last)
		}
	}

	t.Logf("Successfully paginated through %d pages, collected %d items", len(requestLog), len(results))
}

// TestConnector_PagePagination_EdgeCases tests edge cases
func TestConnector_PagePagination_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		totalItems    int
		pageSize      int
		expectedPages int
		expectedItems int
	}{
		{
			name:          "Single page",
			totalItems:    5,
			pageSize:      10,
			expectedPages: 1,
			expectedItems: 5,
		},
		{
			name:          "Exact page boundary",
			totalItems:    20,
			pageSize:      10,
			expectedPages: 2,
			expectedItems: 20,
		},
		{
			name:          "Single item",
			totalItems:    1,
			pageSize:      10,
			expectedPages: 1,
			expectedItems: 1,
		},
		{
			name:          "Large dataset",
			totalItems:    97,
			pageSize:      15,
			expectedPages: 7, // 15*6 + 7 = 97
			expectedItems: 97,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := 0

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				page := r.URL.Query().Get("page")
				pageNum, _ := strconv.Atoi(page)
				if pageNum < 1 {
					pageNum = 1
				}

				startIdx := (pageNum - 1) * tt.pageSize
				endIdx := startIdx + tt.pageSize
				if endIdx > tt.totalItems {
					endIdx = tt.totalItems
				}

				var items []interface{}
				for i := startIdx; i < endIdx; i++ {
					items = append(items, map[string]interface{}{
						"id": i + 1,
					})
				}

				totalPages := (tt.totalItems + tt.pageSize - 1) / tt.pageSize // Ceiling division

				response := map[string]interface{}{
					"items":    items,
					"has_more": pageNum < totalPages,
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "edge-case-test",
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
					Type:        config.PaginationTypePage,
					PageParam:   "page",
					SizeParam:   "size",
					PageSize:    tt.pageSize,
					HasMorePath: "has_more",
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

			if len(results) != tt.expectedItems {
				t.Errorf("Expected %d items, got %d", tt.expectedItems, len(results))
			}

			if requestCount != tt.expectedPages {
				t.Errorf("Expected %d requests, got %d", tt.expectedPages, requestCount)
			}

			t.Logf("%s: %d pages, %d items", tt.name, requestCount, len(results))
		})
	}
}

// TestConnector_PagePagination_ErrorRecovery tests error handling during pagination
func TestConnector_PagePagination_ErrorRecovery(t *testing.T) {
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		page := r.URL.Query().Get("page")

		// Simulate error on page 2
		if page == "2" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "Internal server error"}`))
			return
		}

		// Normal response for page 1
		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"id": 1, "name": "User 1"},
			},
			"has_more": true, // This will cause it to try page 2
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "error-recovery-test",
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
			Type:        config.PaginationTypePage,
			PageParam:   "page",
			SizeParam:   "size",
			PageSize:    10,
			HasMorePath: "has_more",
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error when server returns 500, got nil")
	}

	// Should have made 2 requests (page 1 success, page 2 error)
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Correctly handled server error on page 2: %v", err)
}

// TestConnector_PagePagination_EmptyFirstPage tests empty first page scenario
func TestConnector_PagePagination_EmptyFirstPage(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"items":    []interface{}{}, // Empty items
			"has_more": false,           // No more pages
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "empty-first-page-test",
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
			Type:        config.PaginationTypePage,
			PageParam:   "page",
			SizeParam:   "size",
			PageSize:    10,
			HasMorePath: "has_more",
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
		t.Errorf("Expected 0 results for empty first page, got %d", len(results))
	}

	t.Logf("Correctly handled empty first page")
}
