package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/connector/api"
	"github.com/saturnines/nexus-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// LINK HEADER PAGINATION TESTS

// TestConnector_LinkPagination_Complete tests Link header based pagination
func TestConnector_LinkPagination_Complete(t *testing.T) {
	var requestLog []string
	totalPages := 4
	baseURL := ""

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if baseURL == "" {
			baseURL = "http://" + r.Host
		}

		requestURL := r.URL.String()
		requestLog = append(requestLog, requestURL)

		// Determine page from URL
		pageNum := 1
		if strings.Contains(requestURL, "page=2") {
			pageNum = 2
		} else if strings.Contains(requestURL, "page=3") {
			pageNum = 3
		} else if strings.Contains(requestURL, "page=4") {
			pageNum = 4
		}

		// Generate items for this page
		var items []interface{}
		itemsPerPage := []int{3, 2, 3, 1} // Items for each page
		startID := 1
		for i := 0; i < pageNum-1; i++ {
			startID += itemsPerPage[i]
		}

		for i := 0; i < itemsPerPage[pageNum-1]; i++ {
			itemID := startID + i
			items = append(items, map[string]interface{}{
				"id":   itemID,
				"name": fmt.Sprintf("Link Item %d", itemID),
				"page": pageNum,
			})
		}

		response := map[string]interface{}{
			"data": items,
			"meta": map[string]interface{}{
				"page":        pageNum,
				"total_pages": totalPages,
			},
		}

		// Set Link header for next page
		if pageNum < totalPages {
			nextURL := fmt.Sprintf("%s/data?page=%d", baseURL, pageNum+1)
			lastURL := fmt.Sprintf("%s/data?page=%d", baseURL, totalPages)
			linkHeader := fmt.Sprintf(`<%s>; rel="next", <%s>; rel="last"`, nextURL, lastURL)
			w.Header().Set("Link", linkHeader)
		}
		// No Link header on last page so pagination should stops

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		t.Logf("Served page %d: %d items, Link header: %s", pageNum, len(items), w.Header().Get("Link"))
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "link-pagination-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL + "/data",
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
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx := context.Background()
	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Calculate expected total items
	expectedItems := 9 // because 3 + 2 + 1
	if len(results) != expectedItems {
		t.Errorf("Expected %d total results, got %d", expectedItems, len(results))
	}

	// Verify request sequence
	expectedRequests := []string{
		"/data",        // Initial request
		"/data?page=2", // From Link header
		"/data?page=3", // From Link header
		"/data?page=4", // From Link header
	}

	if len(requestLog) != len(expectedRequests) {
		t.Errorf("Expected %d requests, got %d: %v", len(expectedRequests), len(requestLog), requestLog)
	}

	for i, expected := range expectedRequests {
		if i < len(requestLog) && !strings.HasSuffix(requestLog[i], expected) {
			t.Errorf("Request %d: expected to end with %s, got %s", i+1, expected, requestLog[i])
		}
	}

	// Verify data integrity
	if len(results) > 0 {
		first := results[0]
		if first["id"] != float64(1) || first["name"] != "Link Item 1" {
			t.Errorf("First item incorrect: %+v", first)
		}
	}

	if len(results) == expectedItems {
		last := results[expectedItems-1]
		if last["id"] != float64(expectedItems) || last["name"] != fmt.Sprintf("Link Item %d", expectedItems) {
			t.Errorf("Last item incorrect: %+v", last)
		}
	}

	t.Logf("Successfully link-paginated through %d requests, collected %d items", len(requestLog), len(results))
}

// TestConnector_LinkPagination_MalformedHeaders tests handling of malformed Link headers
func TestConnector_LinkPagination_MalformedHeaders(t *testing.T) {
	tests := []struct {
		name          string
		linkHeaders   []string // Headers for each request
		expectedReqs  int
		expectedItems int
		shouldError   bool
	}{
		{
			name: "Valid then malformed header",
			linkHeaders: []string{
				`</page2>; rel="next"`,                   // Valid
				`malformed link header without brackets`, // Invalid  should stop
			},
			expectedReqs:  2,
			expectedItems: 2, // First page + second page
			shouldError:   false,
		},
		{
			name: "Missing rel attribute",
			linkHeaders: []string{
				`<http://example.com/page2>`, // Missing rel="next"
			},
			expectedReqs:  1,
			expectedItems: 1, // Only first page
			shouldError:   false,
		},
		{
			name: "Empty Link header",
			linkHeaders: []string{
				``, // Empty header
			},
			expectedReqs:  1,
			expectedItems: 1,
			shouldError:   false,
		},
		{
			name: "Multiple rels without next",
			linkHeaders: []string{
				`<http://example.com/first>; rel="first", <http://example.com/last>; rel="last"`, // No "next"
			},
			expectedReqs:  1,
			expectedItems: 1,
			shouldError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := 0

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++

				// Set Link header if we have one for this request
				if requestCount <= len(tt.linkHeaders) && tt.linkHeaders[requestCount-1] != "" {
					w.Header().Set("Link", tt.linkHeaders[requestCount-1])
				}

				response := map[string]interface{}{
					"items": []interface{}{
						map[string]interface{}{
							"id":   requestCount,
							"name": fmt.Sprintf("Item %d", requestCount),
						},
					},
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "malformed-link-test",
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
					Type: config.PaginationTypeLink,
				},
			}

			connector, err := core.NewConnector(cfg)
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

				if requestCount != tt.expectedReqs {
					t.Errorf("Expected %d requests, got %d", tt.expectedReqs, requestCount)
				}
			}

			t.Logf("%s: %d requests, %d items", tt.name, requestCount, len(results))
		})
	}
}

// TestConnector_LinkPagination_RelativeURLs tests relative URLs in Link headers
func TestConnector_LinkPagination_RelativeURLs(t *testing.T) {
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Set relative URL in Link header
		if requestCount == 1 {
			w.Header().Set("Link", `</api/data?page=2>; rel="next"`) // Relative URL
		} else if requestCount == 2 {
			w.Header().Set("Link", `</api/data?page=3>; rel="next"`) // Another relative URL
		}
		// No Link header on third request = stop

		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{
					"id":   requestCount,
					"name": fmt.Sprintf("Relative Item %d", requestCount),
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

		t.Logf("Request %d: %s, Link: %s", requestCount, r.URL.Path, w.Header().Get("Link"))
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "relative-link-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: mockServer.URL + "/api/data",
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	if requestCount != 3 {
		t.Errorf("Expected 3 requests, got %d", requestCount)
	}

	t.Logf(" handled relative URLs in Link headers")
}

// TestConnector_LinkPagination_SinglePage tests single page response with no link header
func TestConnector_LinkPagination_SinglePage(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"id": 1, "name": "Single Item"},
			},
		}
		// No Link header should be a  single page

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "single-page-link-test",
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
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	t.Logf("Correctly handled single page with no Link header")
}

// TestConnector_LinkPagination_ServerError tests server error during Link pagination
func TestConnector_LinkPagination_ServerError(t *testing.T) {
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		if requestCount == 1 {
			// First request succeeds with Link header
			w.Header().Set("Link", fmt.Sprintf(`<%s/page2>; rel="next"`, "http://"+r.Host))
			response := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "name": "First Item"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		} else {
			// Second request fails
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(`{"error": "Gateway timeout"}`))
		}
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "link-error-test",
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
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error when server returns 502, got nil")
	}

	// Should have made 2 requests
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Correctly handled server error during Link pagination: %v", err)
}

// TestConnector_LinkPagination_ComplexHeaders tests complex Link headers with multiple relationships
func TestConnector_LinkPagination_ComplexHeaders(t *testing.T) {
	requestCount := 0
	baseURL := ""

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if baseURL == "" {
			baseURL = "http://" + r.Host
		}

		requestCount++

		// Complex Link headers with multiple rel types
		switch requestCount {
		case 1:
			linkHeader := fmt.Sprintf(
				`<%s/first>; rel="first", <%s/page2>; rel="next", <%s/last>; rel="last", <%s/prev>; rel="prev"`,
				baseURL, baseURL, baseURL, baseURL,
			)
			w.Header().Set("Link", linkHeader)
		case 2:
			linkHeader := fmt.Sprintf(
				`<%s/first>; rel="first", <%s/page1>; rel="prev", <%s/page3>; rel="next", <%s/last>; rel="last"`,
				baseURL, baseURL, baseURL, baseURL,
			)
			w.Header().Set("Link", linkHeader)
		case 3:
			linkHeader := fmt.Sprintf(
				`<%s/first>; rel="first", <%s/page2>; rel="prev", <%s/last>; rel="last"`,
				baseURL, baseURL, baseURL,
			)
			w.Header().Set("Link", linkHeader)
			// No next rel = stop
		}

		response := map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{
					"id":   requestCount,
					"name": fmt.Sprintf("Complex Item %d", requestCount),
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

		t.Logf("Request %d: Link header with %d relationships", requestCount, strings.Count(w.Header().Get("Link"), "rel="))
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "complex-link-test",
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
			Type: config.PaginationTypeLink,
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	results, err := connector.Extract(context.Background())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	if requestCount != 3 {
		t.Errorf("Expected 3 requests, got %d", requestCount)
	}

	t.Logf("Successfully parsed complex Link headers with multiple rel types")
}
