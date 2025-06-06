// tests/integration/reddit_test.go
package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"Nexus/pkg/config"
	"Nexus/pkg/connector/api"
)

// TEST 1: Basic Reddit post extraction from r/programming
func TestReddit_Programming_BasicExtraction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "reddit-programming-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://www.reddit.com/r/programming.json",
			Headers: map[string]string{
				"User-Agent": "NexusConnector/1.0 (Educational Testing)",
			},
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.children",
				Fields: []config.Field{
					{Name: "id", Path: "data.id"},
					{Name: "title", Path: "data.title"},
					{Name: "author", Path: "data.author"},
					{Name: "score", Path: "data.score"},
					{Name: "upvote_ratio", Path: "data.upvote_ratio"},
					{Name: "num_comments", Path: "data.num_comments"},
					{Name: "created_utc", Path: "data.created_utc"},
					{Name: "url", Path: "data.url"},
					{Name: "selftext", Path: "data.selftext"},
					{Name: "subreddit", Path: "data.subreddit"},
					{Name: "permalink", Path: "data.permalink"},
					{Name: "is_video", Path: "data.is_video"},
					{Name: "over_18", Path: "data.over_18"},
					{Name: "locked", Path: "data.locked"},
					{Name: "stickied", Path: "data.stickied"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify we got results
	if len(results) == 0 {
		t.Fatal("Expected at least some posts from r/programming, got 0")
	}

	// Verify essential fields are present
	if len(results) > 0 {
		first := results[0]

		requiredFields := []string{"id", "title", "author", "score", "subreddit"}
		for _, field := range requiredFields {
			if first[field] == nil {
				t.Errorf("Missing required field: %s", field)
			}
		}

		// Verify we're in the correct subreddit
		if first["subreddit"] != "programming" {
			t.Errorf("Expected subreddit 'programming', got: %v", first["subreddit"])
		}

		t.Logf("Successfully extracted %d posts from r/programming", len(results))
		t.Logf("Sample post: %s by %s (score: %v)",
			first["title"], first["author"], first["score"])
	}
}

// TEST 2: Reddit API with query parameters
func TestReddit_Programming_QueryParameters(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "reddit-top-programming-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://www.reddit.com/r/programming/top.json",
			QueryParams: map[string]string{
				"t": "week", // Top posts from this week
			},
			Headers: map[string]string{
				"User-Agent": "NexusConnector/1.0 (Educational Testing)",
			},
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.children",
				Fields: []config.Field{
					{Name: "id", Path: "data.id"},
					{Name: "title", Path: "data.title"},
					{Name: "author", Path: "data.author"},
					{Name: "score", Path: "data.score"},
					{Name: "num_comments", Path: "data.num_comments"},
					{Name: "ups", Path: "data.ups"},
					{Name: "downs", Path: "data.downs"},
					{Name: "upvote_ratio", Path: "data.upvote_ratio"},
					{Name: "gilded", Path: "data.gilded"},
					{Name: "awards", Path: "data.total_awards_received"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected at least some top posts, got 0")
	}

	// Find the highest scoring post to verify sorting
	var maxScore float64 = -1
	var topPost map[string]interface{}

	for _, post := range results {
		if score, ok := post["score"].(float64); ok && score > maxScore {
			maxScore = score
			topPost = post
		}
	}

	if topPost != nil {
		t.Logf("Successfully extracted %d top posts from this week", len(results))
		t.Logf("Highest scoring post: %s (score: %.0f)", topPost["title"], maxScore)

		// Verify the top post has expected engagement metrics
		if topPost["upvote_ratio"] == nil {
			t.Error("Top post missing upvote_ratio field")
		}
		if topPost["num_comments"] == nil {
			t.Error("Top post missing num_comments field")
		}
	}
}

// TEST 3: Complex nested field extraction from different subreddit
func TestReddit_GoLang_NestedFields(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "reddit-golang-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://www.reddit.com/r/golang.json",
			Headers: map[string]string{
				"User-Agent": "NexusConnector/1.0 (Educational Testing)",
			},
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.children",
				Fields: []config.Field{
					{Name: "id", Path: "data.id"},
					{Name: "title", Path: "data.title"},
					{Name: "author", Path: "data.author"},
					{Name: "score", Path: "data.score"},
					{Name: "subreddit", Path: "data.subreddit"},
					{Name: "link_flair_text", Path: "data.link_flair_text"},
					{Name: "domain", Path: "data.domain"},
					{Name: "is_self", Path: "data.is_self"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected at least some golang posts, got 0")
	}

	// Analyze post types and flairs
	var selfPosts, linkPosts int
	var flairCounts = make(map[string]int)

	for _, post := range results {
		if isSelf, ok := post["is_self"].(bool); ok && isSelf {
			selfPosts++
		} else {
			linkPosts++
		}

		if flair, ok := post["link_flair_text"].(string); ok && flair != "" {
			flairCounts[flair]++
		}
	}

	// Verify we're in the correct subreddit
	if len(results) > 0 {
		first := results[0]
		if first["subreddit"] != "golang" {
			t.Errorf("Expected subreddit 'golang', got: %v", first["subreddit"])
		}
	}

	t.Logf("Successfully extracted %d posts from r/golang", len(results))
	t.Logf("Post breakdown: %d self posts, %d link posts", selfPosts, linkPosts)
	if len(flairCounts) > 0 {
		t.Logf("Flairs found: %v", flairCounts)
	}
}

// TEST 4: Error handling for invalid endpoints
func TestReddit_InvalidSubreddit_ErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "reddit-error-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://www.reddit.com/r/thissubredditdoesnotexist123456.json",
			Headers: map[string]string{
				"User-Agent": "NexusConnector/1.0 (Educational Testing)",
			},
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.children",
				Fields: []config.Field{
					{Name: "id", Path: "data.id"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results, err := connector.Extract(ctx)
	if err != nil {
		// Error is expected for invalid subreddit
		t.Logf("Correctly handled invalid subreddit error: %v", err)
		return
	}

	// If no error, results should be empty
	if len(results) > 0 {
		t.Logf("Unexpected: got %d results for invalid subreddit", len(results))
	} else {
		t.Logf("Correctly returned 0 results for invalid subreddit")
	}
}

// TEST 5: Rate limiting behavior testing
func TestReddit_RateLimitBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.Pipeline{
		Name: "reddit-rate-limit-test",
		Source: config.Source{
			Type:     config.SourceTypeREST,
			Endpoint: "https://www.reddit.com/r/programming.json",
			Headers: map[string]string{
				"User-Agent": "NexusConnector/1.0 (Educational Testing)",
			},
			ResponseMapping: config.ResponseMapping{
				RootPath: "data.children",
				Fields: []config.Field{
					{Name: "id", Path: "data.id"},
					{Name: "title", Path: "data.title"},
				},
			},
		},
	}

	connector, err := api.NewConnector(cfg)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	// Make multiple requests to test rate limiting
	successfulRequests := 0
	for i := 1; i <= 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		results, err := connector.Extract(ctx)
		cancel()

		if err != nil {
			// Check if it's a rate limit error
			if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "rate") {
				t.Logf("Encountered rate limiting on request #%d: %v", i, err)
				return
			}
			// Other errors are unexpected
			t.Errorf("Unexpected error on request #%d: %v", i, err)
			return
		}

		successfulRequests++
		t.Logf("Request #%d successful: got %d results", i, len(results))

		// Brief delay between requests
		if i < 3 {
			time.Sleep(2 * time.Second)
		}
	}

	t.Logf("Completed %d successful requests without hitting rate limits", successfulRequests)
}
