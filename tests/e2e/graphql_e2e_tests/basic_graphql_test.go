package graphql_e2e_tests

import (
	"context"
	"encoding/json"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"github.com/saturnines/nexus-core/pkg/errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// DEBUG TEST: Let's see what's actually happening
func TestGraphQL_Debug(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Received request method: %s", r.Method)

		var gqlReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&gqlReq); err != nil {
			t.Errorf("Failed to parse request: %v", err)
			return
		}

		t.Logf("GraphQL Request: %+v", gqlReq)

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{
						"id":   "1",
						"name": "Alice",
					},
					map[string]interface{}{
						"id":   "2",
						"name": "Bob",
					},
				},
			},
		}

		responseBytes, _ := json.MarshalIndent(response, "", "  ")
		t.Logf("Sending response:\n%s", string(responseBytes))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	// Try different RootPath configurations
	testCases := []struct {
		name     string
		rootPath string
	}{
		{"With data.users", "data.users"},
		{"With users only", "users"},
		{"Empty root path", ""},
		{"With data only", "data"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Pipeline{
				Name: "graphql-debug-test",
				Source: config.Source{
					Type: config.SourceTypeGraphQL,
					GraphQLConfig: &config.GraphQLSource{
						Endpoint: mockServer.URL,
						Query:    `query { users { id name } }`,
						ResponseMapping: config.ResponseMapping{
							RootPath: tc.rootPath,
							Fields: []config.Field{
								{Name: "id", Path: "id"},
								{Name: "name", Path: "name"},
							},
						},
					},
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

			t.Logf("RootPath '%s': Got %d results", tc.rootPath, len(results))

			for i, result := range results {
				t.Logf("  Result %d: %+v", i, result)
			}
		})
	}
}

// Also test with a simple working example like your AniList config
func TestGraphQL_SimpleWorking(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mimic AniList-style response
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"Page": map[string]interface{}{
					"media": []interface{}{
						map[string]interface{}{
							"id": 1,
							"title": map[string]interface{}{
								"romaji": "Test Anime 1",
							},
						},
						map[string]interface{}{
							"id": 2,
							"title": map[string]interface{}{
								"romaji": "Test Anime 2",
							},
						},
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-simple-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query { Page { media { id title { romaji } } } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "Page.media", // Like your AniList example
					Fields: []config.Field{
						{Name: "anime_id", Path: "id"},
						{Name: "title_romaji", Path: "title.romaji"},
					},
				},
			},
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

	t.Logf("Got %d results", len(results))
	for i, result := range results {
		t.Logf("Result %d: %+v", i, result)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TEST 1: Basic GraphQL query - simple list extraction
func TestGraphQL_BasicQuery(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify it's a POST request
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Verify Content-Type
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Expected Content-Type 'application/json', got '%s'", ct)
		}

		// Parse the GraphQL request
		var gqlReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&gqlReq); err != nil {
			t.Errorf("Failed to parse GraphQL request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify query exists
		query, ok := gqlReq["query"].(string)
		if !ok || query == "" {
			t.Error("Missing or invalid query field")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Simple response
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{
						"id":    "1",
						"name":  "Alice",
						"email": "alice@example.com",
					},
					map[string]interface{}{
						"id":    "2",
						"name":  "Bob",
						"email": "bob@example.com",
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-basic-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query {
						users {
							id
							name
							email
						}
					}
				`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users", // No 'data' prefix - connector handles that
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "name", Path: "name"},
						{Name: "email", Path: "email"},
					},
				},
			},
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

	// Verify results
	if len(results) != 2 {
		t.Errorf("Expected 2 users, got %d", len(results))
	}

	// Check first user
	if len(results) > 0 {
		first := results[0]
		if first["id"] != "1" {
			t.Errorf("Expected id='1', got %v", first["id"])
		}
		if first["name"] != "Alice" {
			t.Errorf("Expected name='Alice', got %v", first["name"])
		}
		if first["email"] != "alice@example.com" {
			t.Errorf("Expected email='alice@example.com', got %v", first["email"])
		}
	}

	t.Logf("Successfully extracted %d users via GraphQL", len(results))
}

// TEST 2: GraphQL with variables
func TestGraphQL_WithVariables(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var gqlReq map[string]interface{}
		json.NewDecoder(r.Body).Decode(&gqlReq)

		// Check variables were sent
		variables, ok := gqlReq["variables"].(map[string]interface{})
		if !ok {
			t.Error("Missing variables in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify variable values
		if status, ok := variables["status"].(string); !ok || status != "active" {
			t.Errorf("Expected status='active', got %v", variables["status"])
		}
		if limit, ok := variables["limit"].(float64); !ok || limit != 5 {
			t.Errorf("Expected limit=5, got %v", variables["limit"])
		}

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"products": []interface{}{
					map[string]interface{}{
						"id":     "PROD-1",
						"name":   "Widget",
						"status": "active",
						"price":  29.99,
					},
					map[string]interface{}{
						"id":     "PROD-2",
						"name":   "Gadget",
						"status": "active",
						"price":  39.99,
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-variables-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query GetProducts($status: String!, $limit: Int) {
						products(status: $status, limit: $limit) {
							id
							name
							status
							price
						}
					}
				`,
				Variables: map[string]interface{}{
					"status": "active",
					"limit":  5,
				},
				ResponseMapping: config.ResponseMapping{
					RootPath: "products", // No 'data' prefix
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "name", Path: "name"},
						{Name: "status", Path: "status"},
						{Name: "price", Path: "price"},
					},
				},
			},
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

	if len(results) != 2 {
		t.Errorf("Expected 2 products, got %d", len(results))
	}

	// Verify products
	for i, product := range results {
		if product["status"] != "active" {
			t.Errorf("Product %d: expected status='active', got %v", i, product["status"])
		}
		if product["price"] == nil {
			t.Errorf("Product %d: missing price field", i)
		}
	}

	t.Logf("Successfully extracted %d products with variables", len(results))
}

// TEST 3: GraphQL error response handling
func TestGraphQL_ErrorResponse(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return GraphQL error response
		response := map[string]interface{}{
			"errors": []interface{}{
				map[string]interface{}{
					"message": "Cannot query field 'unknown' on type 'User'",
					"locations": []interface{}{
						map[string]interface{}{"line": 3, "column": 5},
					},
					"path": []interface{}{"users", "unknown"},
				},
			},
			"data": nil,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // GraphQL returns 200 even with errors
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-error-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query {
						users {
							id
							name
							unknown
						}
					}
				`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users", // No 'data' prefix
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "name", Path: "name"},
					},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error for GraphQL errors response, got nil")
	}

	// Verify it's a GraphQL error
	if !errors.Is(err, errors.ErrGraphQL) {
		t.Errorf("Expected ErrGraphQL, got error type: %T", err)
	}

	// Verify error message contains GraphQL error details
	if !strings.Contains(err.Error(), "Cannot query field") {
		t.Errorf("Expected error to contain GraphQL error message, got: %s", err.Error())
	}

	t.Logf("Successfully detected GraphQL error: %v", err)
}

// TEST 4: GraphQL with custom headers
func TestGraphQL_CustomHeaders(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify custom headers
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", auth)
		}
		if apiKey := r.Header.Get("X-API-Key"); apiKey != "test-key" {
			t.Errorf("Expected X-API-Key header 'test-key', got '%s'", apiKey)
		}

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"me": map[string]interface{}{
					"id":       "USER-123",
					"username": "testuser",
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-headers-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query { me { id username } }`,
				Headers: map[string]string{
					"Authorization": "Bearer test-token",
					"X-API-Key":     "test-key",
				},
				ResponseMapping: config.ResponseMapping{
					RootPath: "me", // No 'data' prefix
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "username", Path: "username"},
					},
				},
			},
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

	// Since rootPath is "data.me" and it's a single object,
	// the connector should treat it as a single item
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 {
		user := results[0]
		if user["id"] != "USER-123" {
			t.Errorf("Expected id='USER-123', got %v", user["id"])
		}
		if user["username"] != "testuser" {
			t.Errorf("Expected username='testuser', got %v", user["username"])
		}
	}

	t.Logf("Successfully sent custom headers and extracted user data")
}

// TEST 5: Empty GraphQL response
func TestGraphQL_EmptyResponse(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"users": []interface{}{}, // Empty array
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-empty-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query { users { id name } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users", // No 'data' prefix
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "name", Path: "name"},
					},
				},
			},
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

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty response, got %d", len(results))
	}

	t.Logf("Successfully handled empty GraphQL response")
}

// TEST 6: Nested GraphQL response data
func TestGraphQL_NestedData(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"company": map[string]interface{}{
					"id":   "COMP-123",
					"name": "Acme Corp",
					"employees": []interface{}{
						map[string]interface{}{
							"id":    "EMP-1",
							"name":  "John Doe",
							"email": "john@acme.com",
							"department": map[string]interface{}{
								"name": "Engineering",
								"code": "ENG",
							},
						},
						map[string]interface{}{
							"id":    "EMP-2",
							"name":  "Jane Smith",
							"email": "jane@acme.com",
							"department": map[string]interface{}{
								"name": "Marketing",
								"code": "MKT",
							},
						},
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-nested-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query {
						company(id: "COMP-123") {
							id
							name
							employees {
								id
								name
								email
								department {
									name
									code
								}
							}
						}
					}
				`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "company.employees", // No 'data' prefix
					Fields: []config.Field{
						{Name: "employee_id", Path: "id"},
						{Name: "employee_name", Path: "name"},
						{Name: "employee_email", Path: "email"},
						{Name: "department_name", Path: "department.name"},
						{Name: "department_code", Path: "department.code"},
					},
				},
			},
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

	if len(results) != 2 {
		t.Errorf("Expected 2 employees, got %d", len(results))
	}

	// Check nested field extraction
	if len(results) > 0 {
		first := results[0]
		if first["employee_id"] != "EMP-1" {
			t.Errorf("Expected employee_id='EMP-1', got %v", first["employee_id"])
		}
		if first["department_name"] != "Engineering" {
			t.Errorf("Expected department_name='Engineering', got %v", first["department_name"])
		}
		if first["department_code"] != "ENG" {
			t.Errorf("Expected department_code='ENG', got %v", first["department_code"])
		}
	}

	t.Logf("Successfully extracted nested GraphQL data for %d employees", len(results))
}
