package integration

import (
	"context"
	"testing"
	"time"

	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/core"
)

// TEST 1: Basic JSONPlaceholder posts extraction
func TestJSONPlaceholder_Posts_Basic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "jsonplaceholder-posts-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://jsonplaceholder.typicode.com/posts",
			ResponseMapping: config.ResponseMapping{
				// JSONPlaceholder returns array directly (no wrapper)
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "user_id", Path: "userId"},
					{Name: "title", Path: "title"},
					{Name: "body", Path: "body"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// JSONPlaceholder has 100 posts
	if len(results) != 100 {
		t.Errorf("Expected 100 posts, got %d", len(results))
	}

	// Verify first post structure
	if len(results) > 0 {
		first := results[0]
		if first["id"] == nil {
			t.Error("First post missing id field")
		}
		if first["title"] == nil {
			t.Error("First post missing title field")
		}
		if first["user_id"] == nil {
			t.Error("First post missing user_id field")
		}

		t.Logf("First post: ID=%v, Title=%v, UserID=%v",
			first["id"], first["title"], first["user_id"])
	}

	// Verify last post
	if len(results) == 100 {
		last := results[99]
		if last["id"] != float64(100) {
			t.Errorf("Expected last post ID to be 100, got %v", last["id"])
		}

		t.Logf("Last post: ID=%v, Title=%v", last["id"], last["title"])
	}

	t.Logf("🎉 Successfully extracted %d posts from JSONPlaceholder!", len(results))
}

// TEST 2: JSONPlaceholder with pagination
func TestJSONPlaceholder_Posts_WithPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "jsonplaceholder-paginated-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://jsonplaceholder.typicode.com/posts",
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "title", Path: "title"},
				},
			},
		},
		Pagination: &config.Pagination{
			Type:      config.PaginationTypePage,
			PageParam: "_page",
			SizeParam: "_limit",
			PageSize:  10, // Get 10 posts per page
		},
	}

	connector, err := core.NewConnector(cfg) // ← ADD THIS LINE
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ← ADD THIS LINE
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// JSONPlaceholder ignores pagination and returns ALL posts (100)
	expectedCount := 100
	if len(results) != expectedCount {
		t.Errorf("Expected %d posts, got %d", expectedCount, len(results))
	}

	// Verify we got posts 1-100
	if len(results) >= 1 {
		first := results[0]
		if first["id"] != float64(1) {
			t.Errorf("Expected first post ID to be 1, got %v", first["id"])
		}
	}

	if len(results) >= 100 {
		last := results[99]
		if last["id"] != float64(100) {
			t.Errorf("Expected last post ID to be 100, got %v", last["id"])
		}
	}

	t.Logf("🎯 JSONPlaceholder ignored pagination and returned ALL %d posts", len(results))
}

// TEST 3: JSONPlaceholder users with nested data
func TestJSONPlaceholder_Users_NestedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "jsonplaceholder-users-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://jsonplaceholder.typicode.com/users",
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "name", Path: "name"},
					{Name: "username", Path: "username"},
					{Name: "email", Path: "email"},
					{Name: "phone", Path: "phone"},
					// Nested address fields
					{Name: "street", Path: "address.street"},
					{Name: "city", Path: "address.city"},
					{Name: "zipcode", Path: "address.zipcode"},
					// Deeply nested geo coordinates
					{Name: "lat", Path: "address.geo.lat"},
					{Name: "lng", Path: "address.geo.lng"},
					// Company info
					{Name: "company_name", Path: "company.name"},
					{Name: "company_catchphrase", Path: "company.catchPhrase"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// JSONPlaceholder has 10 users
	if len(results) != 10 {
		t.Errorf("Expected 10 users, got %d", len(results))
	}

	// Verify nested field extraction worked
	if len(results) > 0 {
		first := results[0]

		// Basic fields
		if first["name"] == nil {
			t.Error("Missing name field")
		}
		if first["email"] == nil {
			t.Error("Missing email field")
		}

		// Nested address fields
		if first["street"] == nil {
			t.Error("Missing nested street field")
		}
		if first["city"] == nil {
			t.Error("Missing nested city field")
		}

		// Deeply nested geo fields
		if first["lat"] == nil {
			t.Error("Missing deeply nested lat field")
		}
		if first["lng"] == nil {
			t.Error("Missing deeply nested lng field")
		}

		// Company fields
		if first["company_name"] == nil {
			t.Error("Missing company name field")
		}

		t.Logf("First user: %s (%s) from %s, %s",
			first["name"], first["email"], first["city"], first["company_name"])
		t.Logf("Location: %s, %s (lat: %s, lng: %s)",
			first["street"], first["zipcode"], first["lat"], first["lng"])
	}

	t.Logf("🌟 Successfully extracted nested fields from %d users!", len(results))
}

// TEST 4: JSONPlaceholder comments with query parameters
func TestJSONPlaceholder_Comments_WithQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "jsonplaceholder-comments-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://jsonplaceholder.typicode.com/comments",
			QueryParams: map[string]string{
				"postId": "1", // Only get comments for post 1
			},
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
					{Name: "post_id", Path: "postId"},
					{Name: "name", Path: "name"},
					{Name: "email", Path: "email"},
					{Name: "body", Path: "body"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Should get 5 comments for post 1
	expectedCount := 5
	if len(results) != expectedCount {
		t.Errorf("Expected %d comments, got %d", expectedCount, len(results))
	}

	// Verify all comments are for post 1
	for i, comment := range results {
		if comment["post_id"] != float64(1) {
			t.Errorf("Comment %d has wrong post_id: expected 1, got %v", i, comment["post_id"])
		}
	}

	if len(results) > 0 {
		first := results[0]
		t.Logf("First comment: %s (%s) - %v",
			first["name"], first["email"], first["body"])
	}

	t.Logf("✨ Successfully filtered and extracted %d comments for post 1!", len(results))
}

// TEST 5: Error handling with invalid endpoint
func TestJSONPlaceholder_InvalidEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "jsonplaceholder-error-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://jsonplaceholder.typicode.com/nonexistent",
			ResponseMapping: config.ResponseMapping{
				Fields: []config.Field{
					{Name: "id", Path: "id"},
				},
			},
		},
	}

	connector, err := core.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = connector.Extract(ctx)
	if err == nil {
		t.Fatal("Expected error for invalid endpoint, got nil")
	}

	t.Logf("🚨 Correctly handled invalid endpoint error: %v", err)
}
