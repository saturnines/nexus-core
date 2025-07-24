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

func TestGraphQL_APIKeyAuth(t *testing.T) {
	tests := []struct {
		name        string
		headerName  string
		queryParam  string
		apiKey      string
		checkHeader bool
		checkQuery  bool
	}{
		{
			name:        "Header based API key",
			headerName:  "X-API-Key",
			apiKey:      "test-api-key-123",
			checkHeader: true,
		},
		{
			name:       "Query param API key",
			queryParam: "api_key",
			apiKey:     "test-api-key-456",
			checkQuery: true,
		},
		{
			name:        "Both header and query",
			headerName:  "X-API-Key",
			queryParam:  "api_key",
			apiKey:      "test-api-key-789",
			checkHeader: true,
			checkQuery:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Check header if needed
				if tt.checkHeader {
					if r.Header.Get(tt.headerName) != tt.apiKey {
						t.Errorf("Expected header %s='%s', got '%s'",
							tt.headerName, tt.apiKey, r.Header.Get(tt.headerName))
					}
				}

				// Check query param if needed
				if tt.checkQuery {
					if r.URL.Query().Get(tt.queryParam) != tt.apiKey {
						t.Errorf("Expected query param %s='%s', got '%s'",
							tt.queryParam, tt.apiKey, r.URL.Query().Get(tt.queryParam))
					}
				}

				response := map[string]interface{}{
					"data": map[string]interface{}{
						"apiInfo": map[string]interface{}{
							"version": "1.0",
							"status":  "authenticated",
						},
					},
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			}))
			defer mockServer.Close()

			cfg := &config.Pipeline{
				Name: "graphql-apikey-test",
				Source: config.Source{
					Type: config.SourceTypeGraphQL,
					GraphQLConfig: &config.GraphQLSource{
						Endpoint: mockServer.URL,
						Query:    `query { apiInfo { version status } }`,
						ResponseMapping: config.ResponseMapping{
							RootPath: "apiInfo",
							Fields: []config.Field{
								{Name: "version", Path: "version"},
								{Name: "status", Path: "status"},
							},
						},
					},
					Auth: &config.Auth{
						Type: config.AuthTypeAPIKey,
						APIKey: &config.APIKeyAuth{
							Header:     tt.headerName,
							QueryParam: tt.queryParam,
							Value:      tt.apiKey,
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

			if len(results) != 1 {
				t.Errorf("Expected 1 result, got %d", len(results))
			}
		})
	}
}
