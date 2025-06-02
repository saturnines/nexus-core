package api

import (
	"Nexus/pkg/config"
	"Nexus/pkg/connector/api"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// OFFSET PAGINATION TESTS

// TestConnector_OffsetPagination_Complete tests offset based pagination
func TestConnector_OffsetPagination_Complete(t *testing.T) {
	var requestLog []string
	totalItems := 47
	pageSize := 15

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		limit := r.URL.Query().Get("limit")
		requestLog = append(requestLog, fmt.Sprintf("offset=%s&limit=%s", offset, limit))

		offsetNum, err := strconv.Atoi(offset)
		if err != nil || offsetNum < 0 {
			offsetNum = 0
		}

		limitNum, err := strconv.Atoi(limit)
		if err != nil || limitNum < 1 {
			limitNum = pageSize
		}

		// Calculate items for this offset
		endIdx := offsetNum + limitNum
		if endIdx > totalItems {
			endIdx = totalItems
		}

		var items []interface{}
		for i := offsetNum; i < endIdx; i++ {
			items = append(items, map[string]interface{}{
				"id":     i + 1,
				"name":   fmt.Sprintf("Record %d", i+1),
				"offset": offsetNum,
			})
		}

		hasMore := endIdx < totalItems

		response := map[string]interface{}{
			"data": items,
			"meta": map[string]interface{}{
				"total_count": totalItems,
				"offset":      offsetNum,
				"limit":       limitNum,
				"returned":    len(items),
			},
			"has_more": hasMore,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		t.Logf("Served offset %d: %d items (IDs %d-%d), has_more=%t", offsetNum, len(items), offsetNum+1, endIdx, hasMore)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "offset-pagination-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					{Name: "offset", Path: "offset"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:            config.PaginationTypeOffset,
			OffsetParam:     "offset",
			LimitParam:      "limit",
			OffsetIncrement: pageSize,
			HasMorePath:     "has_more",
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
		"offset=0&limit=15",
		"offset=15&limit=15",
		"offset=30&limit=15",
		"offset=45&limit=15",
	}

	if len(requestLog) != len(expectedRequests) {
		t.Errorf("Expected %d requests, got %d: %v", len(expectedRequests), len(requestLog), requestLog)
	}

	for i, expected := range expectedRequests {
		if i < len(requestLog) && requestLog[i] != expected {
			t.Errorf("Request %d: expected %s, got %s", i+1, expected, requestLog[i])
		}
	}

	// Verify data integrity
	if len(results) > 0 {
		first := results[0]
		if first["id"] != float64(1) || first["name"] != "Record 1" {
			t.Errorf("First item incorrect: %+v", first)
		}
	}

	if len(results) >= 25 {
		middle := results[24] // 25th item
		if middle["id"] != float64(25) || middle["name"] != "Record 25" {
			t.Errorf("Middle item incorrect: %+v", middle)
		}
	}

	if len(results) == totalItems {
		last := results[totalItems-1]
		if last["id"] != float64(totalItems) || last["name"] != fmt.Sprintf("Record %d", totalItems) {
			t.Errorf("Last item incorrect: %+v", last)
		}
	}

	t.Logf("Successfully offset paginated through %d requests, collected %d items", len(requestLog), len(results))
}

// TestConnector_OffsetPagination_EdgeCases tests offset pagination edge cases
func TestConnector_OffsetPagination_EdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		totalItems       int
		offsetIncrement  int
		expectedRequests int
		expectedItems    int
	}{
		{
			name:             "Small dataset",
			totalItems:       7,
			offsetIncrement:  10,
			expectedRequests: 1,
			expectedItems:    7,
		},
		{
			name:             "Exact increment boundary",
			totalItems:       30,
			offsetIncrement:  10,
			expectedRequests: 3,
			expectedItems:    30,
		},
		{
			name:             "Large increment",
			totalItems:       100,
			offsetIncrement:  25,
			expectedRequests: 4,
			expectedItems:    100,
		},
		{
			name:             "Single item per request",
			totalItems:       5,
			offsetIncrement:  1,
			expectedRequests: 5,
			expectedItems:    5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := 0

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				offset := r.URL.Query().Get("offset")
				limit := r.URL.Query().Get("limit")

				offsetNum, _ := strconv.Atoi(offset)
				if offsetNum < 0 {
					offsetNum = 0
				}

				limitNum, _ := strconv.Atoi(limit)
				if limitNum < 1 {
					limitNum = tt.offsetIncrement
				}

				endIdx := offsetNum + limitNum
				if endIdx > tt.totalItems {
					endIdx = tt.totalItems
				}

				var items []interface{}
				for i := offsetNum; i < endIdx; i++ {
					items = append(items, map[string]interface{}{
						"id": i + 1,
					})
				}

				response := map[string]interface{}{
					"data":     items,
					"has_more": endIdx < tt.totalItems,
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "offset-edge-case-test",
				Source: config.Source{
					Type:     config.SourceTypeREST,
					Endpoint: mockServer.URL,
					ResponseMapping: config.ResponseMapping{
						RootPath: "data",
						Fields: []config.Field{
							{Name: "id", Path: "id"},
						},
					},
				},
				Pagination: &config.Pagination{
					Type:            config.PaginationTypeOffset,
					OffsetParam:     "offset",
					LimitParam:      "limit",
					OffsetIncrement: tt.offsetIncrement,
					HasMorePath:     "has_more",
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

			if requestCount != tt.expectedRequests {
				t.Errorf("Expected %d requests, got %d", tt.expectedRequests, requestCount)
			}

			t.Logf("%s: %d requests, %d items", tt.name, requestCount, len(results))
		})
	}
}

// TestConnector_OffsetPagination_ZeroResults tests empty dataset
func TestConnector_OffsetPagination_ZeroResults(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": []interface{}{}, // Empty data
			"meta": map[string]interface{}{
				"total_count": 0,
			},
			"has_more": false,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "offset-zero-results-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:            config.PaginationTypeOffset,
			OffsetParam:     "offset",
			LimitParam:      "limit",
			OffsetIncrement: 10,
			HasMorePath:     "has_more",
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

	t.Logf("Correctly handled zero results")
}

// TestConnector_OffsetPagination_ServerError tests error during offset pagination (todo, write randomized tests for tests.)
func TestConnector_OffsetPagination_ServerError(t *testing.T) {
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		offset := r.URL.Query().Get("offset")

		// Simulate error on second request (offset=10)
		if offset == "10" {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error": "Database temporarily unavailable"}`))
			return
		}

		// Normal response for first request
		response := map[string]interface{}{
			"data": []interface{}{
				map[string]interface{}{"id": 1, "name": "Record 1"},
				map[string]interface{}{"id": 2, "name": "Record 2"},
			},
			"has_more": true, // This will cause it to try offset=10
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "offset-error-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL,
			ResponseMapping: config.ResponseMapping{
				RootPath: "data",
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:            config.PaginationTypeOffset,
			OffsetParam:     "offset",
			LimitParam:      "limit",
			OffsetIncrement: 10,
			HasMorePath:     "has_more",
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error when server returns 502, got nil")
	}

	// Should have made 2 requests 0 is success.
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Correctly handled server error on second offset request: %v", err)
}
