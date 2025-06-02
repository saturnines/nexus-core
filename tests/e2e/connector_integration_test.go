package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// TEST 6: Different Response Structures (No items key)
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

// TEST 7: Pagination Test - Page-based
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
