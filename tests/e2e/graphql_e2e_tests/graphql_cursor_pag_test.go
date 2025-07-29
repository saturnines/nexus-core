package graphql_e2e_tests

import (
	"context"
	"encoding/json"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TEST 1: Basic GraphQL cursor pagination
func TestGraphQL_CursorPagination_Basic(t *testing.T) {
	requestCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Parse GraphQL request
		var gqlReq map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&gqlReq); err != nil {
			t.Errorf("Failed to parse GraphQL request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Check variables
		variables, _ := gqlReq["variables"].(map[string]interface{})
		cursor, _ := variables["cursor"].(string)

		t.Logf("Request %d: cursor='%s'", requestCount, cursor)

		var response map[string]interface{}

		switch cursor {
		case "":
			// First page
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"users": map[string]interface{}{
						"edges": []interface{}{
							map[string]interface{}{
								"node": map[string]interface{}{
									"id":   "1",
									"name": "Alice",
								},
								"cursor": "cursor_1",
							},
							map[string]interface{}{
								"node": map[string]interface{}{
									"id":   "2",
									"name": "Bob",
								},
								"cursor": "cursor_2",
							},
						},
						"pageInfo": map[string]interface{}{
							"endCursor":   "cursor_2",
							"hasNextPage": true,
						},
					},
				},
			}
		case "cursor_2":
			// Second page
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"users": map[string]interface{}{
						"edges": []interface{}{
							map[string]interface{}{
								"node": map[string]interface{}{
									"id":   "3",
									"name": "Charlie",
								},
								"cursor": "cursor_3",
							},
						},
						"pageInfo": map[string]interface{}{
							"endCursor":   "cursor_3",
							"hasNextPage": false,
						},
					},
				},
			}
		default:
			t.Errorf("Unexpected cursor: %s", cursor)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-cursor-pagination-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query GetUsers($cursor: String) {
						users(after: $cursor, first: 10) {
							edges {
								node {
									id
									name
								}
								cursor
							}
							pageInfo {
								endCursor
								hasNextPage
							}
						}
					}
				`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users.edges",
					Fields: []config.Field{
						{Name: "id", Path: "node.id"},
						{Name: "name", Path: "node.name"},
						{Name: "cursor", Path: "cursor"},
					},
				},
			},
		},
		// MOVE PAGINATION HERE - at Pipeline level
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "data.users.pageInfo.endCursor",   // Note: needs "data." prefix
			HasMorePath: "data.users.pageInfo.hasNextPage", // Note: needs "data." prefix
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

	// Should get 3 total results from 2 pages
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Should have made 2 requests
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	// Verify data
	expectedNames := []string{"Alice", "Bob", "Charlie"}
	for i, result := range results {
		if i < len(expectedNames) {
			if result["name"] != expectedNames[i] {
				t.Errorf("Result %d: expected name='%s', got %v", i, expectedNames[i], result["name"])
			}
		}
	}

	t.Logf("Successfully paginated through %d pages, got %d results", requestCount, len(results))
}

// TEST 2: GraphQL cursor pagination with complex nested structure
func TestGraphQL_CursorPagination_ComplexNesting(t *testing.T) {
	requestCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		var gqlReq map[string]interface{}
		json.NewDecoder(r.Body).Decode(&gqlReq)
		variables, _ := gqlReq["variables"].(map[string]interface{})
		cursor, _ := variables["after"].(string)

		t.Logf("Complex nesting request %d: after='%s'", requestCount, cursor)

		var response map[string]interface{}

		if cursor == "" {
			// First page - GitHub-style response
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"repository": map[string]interface{}{
						"issues": map[string]interface{}{
							"totalCount": 100,
							"edges": []interface{}{
								map[string]interface{}{
									"node": map[string]interface{}{
										"id":     "ISSUE-1",
										"title":  "Bug: Memory leak",
										"number": 123,
										"author": map[string]interface{}{
											"login": "alice",
										},
									},
									"cursor": "Y3Vyc29yOnYyOpHOAQ==",
								},
								map[string]interface{}{
									"node": map[string]interface{}{
										"id":     "ISSUE-2",
										"title":  "Feature: Dark mode",
										"number": 124,
										"author": map[string]interface{}{
											"login": "bob",
										},
									},
									"cursor": "Y3Vyc29yOnYyOpHOAg==",
								},
							},
							"pageInfo": map[string]interface{}{
								"startCursor":     "Y3Vyc29yOnYyOpHOAQ==",
								"endCursor":       "Y3Vyc29yOnYyOpHOAg==",
								"hasNextPage":     true,
								"hasPreviousPage": false,
							},
						},
					},
				},
			}
		} else if cursor == "Y3Vyc29yOnYyOpHOAg==" {
			// Second page
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"repository": map[string]interface{}{
						"issues": map[string]interface{}{
							"totalCount": 100,
							"edges": []interface{}{
								map[string]interface{}{
									"node": map[string]interface{}{
										"id":     "ISSUE-3",
										"title":  "Docs: Update README",
										"number": 125,
										"author": map[string]interface{}{
											"login": "charlie",
										},
									},
									"cursor": "Y3Vyc29yOnYyOpHOAw==",
								},
							},
							"pageInfo": map[string]interface{}{
								"startCursor":     "Y3Vyc29yOnYyOpHOAw==",
								"endCursor":       "Y3Vyc29yOnYyOpHOAw==",
								"hasNextPage":     false,
								"hasPreviousPage": true,
							},
						},
					},
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-complex-cursor-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
                    query GetIssues($after: String) {
                        repository(owner: "example", name: "repo") {
                            issues(first: 10, after: $after) {
                                totalCount
                                edges {
                                    node {
                                        id
                                        title
                                        number
                                        author {
                                            login
                                        }
                                    }
                                    cursor
                                }
                                pageInfo {
                                    startCursor
                                    endCursor
                                    hasNextPage
                                    hasPreviousPage
                                }
                            }
                        }
                    }
                `,
				Variables: map[string]interface{}{
					"after": nil, // Will be set by pagination
				},
				ResponseMapping: config.ResponseMapping{
					RootPath: "repository.issues.edges",
					Fields: []config.Field{
						{Name: "id", Path: "node.id"},
						{Name: "title", Path: "node.title"},
						{Name: "number", Path: "node.number"},
						{Name: "author", Path: "node.author.login"},
						{Name: "cursor", Path: "cursor"},
					},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "after", // Match the GraphQL variable name!
			CursorPath:  "data.repository.issues.pageInfo.endCursor",
			HasMorePath: "data.repository.issues.pageInfo.hasNextPage",
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
		t.Errorf("Expected 3 issues, got %d", len(results))
	}

	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	// Verify nested field extraction
	if len(results) > 0 {
		first := results[0]
		if first["author"] != "alice" {
			t.Errorf("Expected first issue author='alice', got %v", first["author"])
		}
	}

	t.Logf("Successfully handled complex nested cursor pagination")
}

// TEST 3: Empty dataset with cursor pagination
func TestGraphQL_CursorPagination_EmptyDataset(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"users": map[string]interface{}{
					"edges": []interface{}{}, // Empty
					"pageInfo": map[string]interface{}{
						"endCursor":   nil,
						"hasNextPage": false,
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-cursor-empty-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query: `
					query GetUsers($cursor: String) {
						users(after: $cursor) {
							edges {
								node {
									id
								}
							}
							pageInfo {
								endCursor
								hasNextPage
							}
						}
					}
				`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users.edges",
					Fields: []config.Field{
						{Name: "id", Path: "node.id"},
					},
				},
				Pagination: &config.Pagination{
					Type:        config.PaginationTypeCursor,
					CursorParam: "cursor",
					CursorPath:  "users.pageInfo.endCursor",
					HasMorePath: "users.pageInfo.hasNextPage",
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
		t.Errorf("Expected 0 results for empty dataset, got %d", len(results))
	}

	t.Logf("Successfully handled empty dataset with cursor pagination")
}

// TEST 4: Cursor pagination error handling
func TestGraphQL_CursorPagination_ErrorHandling(t *testing.T) {
	requestCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		var gqlReq map[string]interface{}
		json.NewDecoder(r.Body).Decode(&gqlReq)
		variables, _ := gqlReq["variables"].(map[string]interface{})
		cursor, _ := variables["cursor"].(string)

		if cursor == "" {
			// First page succeeds
			response := map[string]interface{}{
				"data": map[string]interface{}{
					"users": map[string]interface{}{
						"edges": []interface{}{
							map[string]interface{}{
								"node": map[string]interface{}{"id": "1"},
							},
						},
						"pageInfo": map[string]interface{}{
							"endCursor":   "cursor_1",
							"hasNextPage": true,
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		} else {
			// Second page returns error
			response := map[string]interface{}{
				"errors": []interface{}{
					map[string]interface{}{
						"message": "Invalid cursor",
						"extensions": map[string]interface{}{
							"code": "BAD_USER_INPUT",
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK) // GraphQL returns 200 with errors
			json.NewEncoder(w).Encode(response)
		}
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-cursor-error-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query GetUsers($cursor: String) { users(after: $cursor) { edges { node { id } } pageInfo { endCursor hasNextPage } } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "users.edges",
					Fields: []config.Field{
						{Name: "id", Path: "node.id"},
					},
				},
			},
		},

		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "data.users.pageInfo.endCursor",
			HasMorePath: "data.users.pageInfo.hasNextPage", // Note: needs "data." prefix
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	_, err = connector.Extract(context.Background())
	if err == nil {
		t.Fatal("Expected error during pagination, got nil")
	}

	if requestCount != 2 {
		t.Errorf("Expected 2 requests before error, got %d", requestCount)
	}

	t.Logf("Successfully handled error during cursor pagination: %v", err)
}

// TEST 5: Cursor pagination with special characters
func TestGraphQL_CursorPagination_SpecialCharacters(t *testing.T) {
	specialCursor := "Y3Vyc29yOnYyOpK5MjAyMy0xMi0yNVQxMDozMDowMCswMDowMM4A8J+RjQ=="
	requestCount := 0

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		var gqlReq map[string]interface{}
		json.NewDecoder(r.Body).Decode(&gqlReq)
		variables, _ := gqlReq["variables"].(map[string]interface{})
		cursor, _ := variables["cursor"].(string)

		t.Logf("Received cursor: '%s'", cursor)

		var response map[string]interface{}

		if cursor == "" {
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"items": map[string]interface{}{
						"edges": []interface{}{
							map[string]interface{}{
								"node": map[string]interface{}{"id": "1"},
							},
						},
						"pageInfo": map[string]interface{}{
							"endCursor":   specialCursor,
							"hasNextPage": true,
						},
					},
				},
			}
		} else if cursor == specialCursor {
			response = map[string]interface{}{
				"data": map[string]interface{}{
					"items": map[string]interface{}{
						"edges": []interface{}{
							map[string]interface{}{
								"node": map[string]interface{}{"id": "2"},
							},
						},
						"pageInfo": map[string]interface{}{
							"endCursor":   nil,
							"hasNextPage": false,
						},
					},
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-cursor-special-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query($cursor: String) { items(after: $cursor) { edges { node { id } } pageInfo { endCursor hasNextPage } } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "items.edges",
					Fields: []config.Field{
						{Name: "id", Path: "node.id"},
					},
				},
			},
		},
		// MOVE PAGINATION HERE - at Pipeline level
		Pagination: &config.Pagination{
			Type:        config.PaginationTypeCursor,
			CursorParam: "cursor",
			CursorPath:  "data.items.pageInfo.endCursor",   // Note: needs "data." prefix
			HasMorePath: "data.items.pageInfo.hasNextPage", // Note: needs "data." prefix
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
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}

	t.Logf("Successfully handled cursor with special characters")
}
