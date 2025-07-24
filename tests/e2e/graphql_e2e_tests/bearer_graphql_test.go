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

func TestGraphQL_BearerTokenAuth(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token-123" {
			response := map[string]interface{}{
				"errors": []interface{}{
					map[string]interface{}{
						"message": "Invalid token",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"protectedData": []interface{}{
					map[string]interface{}{"id": "1", "value": "secret1"},
					map[string]interface{}{"id": "2", "value": "secret2"},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-bearer-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query { protectedData { id value } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "protectedData",
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "value", Path: "value"},
					},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeBearer,
				Bearer: &config.BearerAuth{
					Token: "test-token-123",
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
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}
