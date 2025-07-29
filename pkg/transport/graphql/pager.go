package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/errors"
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
	mu         sync.RWMutex
	hasNext    bool
	first      bool
	nextCursor string
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
		return nil, errors.WrapError(
			fmt.Errorf("builder cannot be nil"),
			errors.ErrConfiguration,
			"create GraphQL pager",
		)
	}
	if client == nil {
		return nil, errors.WrapError(
			fmt.Errorf("client cannot be nil"),
			errors.ErrConfiguration,
			"create GraphQL pager",
		)
	}
	if cursorKey == "" {
		return nil, errors.WrapError(
			fmt.Errorf("cursorKey cannot be empty"),
			errors.ErrConfiguration,
			"create GraphQL pager",
		)
	}
	if len(nextPath) == 0 {
		return nil, errors.WrapError(
			fmt.Errorf("nextPath cannot be empty"),
			errors.ErrConfiguration,
			"create GraphQL pager",
		)
	}
	if len(hasNextPath) == 0 {
		return nil, errors.WrapError(
			fmt.Errorf("hasNextPath cannot be empty"),
			errors.ErrConfiguration,
			"create GraphQL pager",
		)
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
	nextCursor := p.nextCursor
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

	// Add cursor for subsequent requests
	if !first && nextCursor != "" {
		builderCopy.Variables[p.cursorKey] = nextCursor
	}

	// Use a dummy context - the caller will provide the real context
	return builderCopy.Build(context.Background())
}

// UpdateState reads pageInfo from resp, updates cursor and hasNext.
func (p *GraphQLPager) UpdateState(resp *http.Response) error {
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return errors.WrapError(err, errors.ErrHTTPResponse, "decode GraphQL pager response")
	}

	if errorsField, ok := data["errors"]; ok && errorsField != nil {
		bodyBytes, err := json.Marshal(data)
		if err != nil {
			return errors.WrapError(err, errors.ErrHTTPResponse, "marshal GraphQL error response")
		}
		if err := errors.CheckGraphQLErrors(bodyBytes); err != nil {
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Mark that we've made the first request
	p.first = false

	// Extract endCursor and store it separately (don't mutate builder)
	endCursor := traverse(data, p.nextPath...)
	if str, ok := endCursor.(string); ok && str != "" {
		p.nextCursor = str
	} else {
		p.nextCursor = ""
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
	p.nextCursor = ""
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
