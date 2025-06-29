package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/saturnines/nexus-core/pkg/pagination"
)

// GraphQLPager drives cursor paging in GraphQL with thread safety.
type GraphQLPager struct {
	// Immutable configuration
	builder     *Builder
	client      *Client
	cursorKey   string
	nextPath    []string
	hasNextPath []string

	// Mutable state (protected by mutex)
	mu      sync.RWMutex
	hasNext bool
	first   bool
}

// NewPager returns a pagination.Pager for GraphQL cursor paging.
// Does NOT execute any requests during creation.
func NewPager(
	ctx context.Context,
	builder *Builder,
	client *Client,
	cursorKey string,
	nextPath, hasNextPath []string,
) (pagination.Pager, error) {
	// Validate inputs
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if cursorKey == "" {
		return nil, fmt.Errorf("cursorKey cannot be empty")
	}
	if len(nextPath) == 0 {
		return nil, fmt.Errorf("nextPath cannot be empty")
	}
	if len(hasNextPath) == 0 {
		return nil, fmt.Errorf("hasNextPath cannot be empty")
	}

	return &GraphQLPager{
		builder:     builder,
		client:      client,
		cursorKey:   cursorKey,
		nextPath:    nextPath,
		hasNextPath: hasNextPath,
		hasNext:     true,
		first:       true,
	}, nil
}

// NextRequest builds the next *http.Request or returns (nil,nil) when done.
func (p *GraphQLPager) NextRequest() (*http.Request, error) {
	p.mu.RLock()
	hasNext := p.hasNext
	first := p.first
	p.mu.RUnlock()

	if !first && !hasNext {
		return nil, nil
	}

	// Create a copy of the builder to avoid modifying the original
	builderCopy := &Builder{
		Endpoint:    p.builder.Endpoint,
		Query:       p.builder.Query,
		Variables:   make(map[string]interface{}),
		Headers:     p.builder.Headers,
		AuthHandler: p.builder.AuthHandler,
	}

	// Copy variables
	for k, v := range p.builder.Variables {
		builderCopy.Variables[k] = v
	}

	// Use a dummy context - the caller will provide the real context
	return builderCopy.Build(context.Background())
}

// UpdateState reads pageInfo from resp, updates cursor and hasNext.
func (p *GraphQLPager) UpdateState(resp *http.Response) error {
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode GraphQL response: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Mark that we've made the first request
	p.first = false

	// Extract endCursor and update builder variables
	endCursor := traverse(data, p.nextPath...)
	if str, ok := endCursor.(string); ok && str != "" {
		// Update the builder's variables for the next request
		if p.builder.Variables == nil {
			p.builder.Variables = make(map[string]interface{})
		}
		p.builder.Variables[p.cursorKey] = str
	}

	// Extract hasNextPage
	hasNext := traverse(data, p.hasNextPath...)
	if b, ok := hasNext.(bool); ok {
		p.hasNext = b
	} else {
		// If we can't determine hasNext, assume no more pages
		p.hasNext = false
	}

	return nil
}

// HasMore returns whether more pages are available (thread-safe).
func (p *GraphQLPager) HasMore() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hasNext
}

// Reset resets pagination to start from the beginning.
func (p *GraphQLPager) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.hasNext = true
	p.first = true

	// Remove cursor from variables
	if p.builder.Variables != nil {
		delete(p.builder.Variables, p.cursorKey)
	}
}

// traverse digs into nested maps via a path of keys.
func traverse(m map[string]interface{}, path ...string) interface{} {
	cur := interface{}(m)
	for _, key := range path {
		if mp, ok := cur.(map[string]interface{}); ok {
			cur = mp[key]
		} else {
			return nil
		}
	}
	return cur
}
