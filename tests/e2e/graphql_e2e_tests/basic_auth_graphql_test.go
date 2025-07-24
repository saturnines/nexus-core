package graphql_e2e_tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGraphQL_BasicAuthentication(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Basic Auth header
		auth := r.Header.Get("Authorization")
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("testuser:testpass"))

		if auth != expectedAuth {
			response := map[string]interface{}{
				"errors": []interface{}{
					map[string]interface{}{
						"message": "Authentication required",
						"extensions": map[string]interface{}{
							"code": "UNAUTHENTICATED",
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		response := map[string]interface{}{
			"data": map[string]interface{}{
				"currentUser": map[string]interface{}{
					"id":   "123",
					"name": "Test User",
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Pipeline{
		Name: "graphql-basic-auth-test",
		Source: config.Source{
			Type: config.SourceTypeGraphQL,
			GraphQLConfig: &config.GraphQLSource{
				Endpoint: mockServer.URL,
				Query:    `query { currentUser { id name } }`,
				ResponseMapping: config.ResponseMapping{
					RootPath: "currentUser",
					Fields: []config.Field{
						{Name: "id", Path: "id"},
						{Name: "name", Path: "name"},
					},
				},
			},
			Auth: &config.Auth{
				Type: config.AuthTypeBasic,
				Basic: &config.BasicAuth{
					Username: "testuser",
					Password: "testpass",
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
}
