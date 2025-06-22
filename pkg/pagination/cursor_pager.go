package pagination

import (
	"fmt"
	"net/http"
	"sync"
)

// ThreadSafeCursorPager handles cursor pagination
// This implementation is/should thread safe?
type ThreadSafeCursorPager struct {
	// Immutable configuration (no mutex needed)
	client      HTTPDoer
	baseReq     *http.Request
	cursorParam string
	nextPath    string

	// Mutable state (protected by mutex)
	mu         sync.RWMutex
	nextCursor string
	hasMore    bool
	first      bool
}

// NewThreadSafeCursorPager creates a cursor pager.
// Returns error if nextPath is empty.
func NewThreadSafeCursorPager(client HTTPDoer, req *http.Request, cursorParam, nextPath string) (*ThreadSafeCursorPager, error) {
	if nextPath == "" {
		return nil, fmt.Errorf("nextPath cannot be empty")
	}

	return &ThreadSafeCursorPager{
		client:      client,
		baseReq:     req,
		cursorParam: cursorParam,
		nextPath:    nextPath,
		hasMore:     true,
		first:       true,
	}, nil
}

// NextRequest returns the next HTTP request, or nil when there are no more pages.
func (p *ThreadSafeCursorPager) NextRequest() (*http.Request, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if pagination is complete
	if !p.first && !p.hasMore {
		return nil, nil
	}

	// Build the request
	req := p.baseReq.Clone(p.baseReq.Context())

	// Add cursor parameter for subsequent requests
	if !p.first && p.nextCursor != "" {
		q := req.URL.Query()
		q.Set(p.cursorParam, p.nextCursor)
		req.URL.RawQuery = q.Encode()
	}

	p.first = false
	return req, nil
}

// UpdateState processes response and updates pagination state.
func (p *ThreadSafeCursorPager) UpdateState(resp *http.Response) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate response
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cursor pagination failed: HTTP %d", resp.StatusCode)
	}

	// Parse response body
	body, err := parseBody(resp)
	if err != nil {
		return fmt.Errorf("cursor pagination: failed to parse response: %w", err)
	}

	// Extract next cursor value
	nextCursorValue, err := lookupString(body, p.nextPath)

	// Update pagination state based on next cursor
	if err != nil || nextCursorValue == "" {
		// Field missing or empty - no more pages
		p.nextCursor = ""
		p.hasMore = false
	} else {
		// Valid next cursor - continue pagination
		p.nextCursor = nextCursorValue
		p.hasMore = true
	}

	return nil
}

// HasMore returns whether more pages are available.
func (p *ThreadSafeCursorPager) HasMore() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hasMore
}

// GetNextCursor returns the next cursor value.
func (p *ThreadSafeCursorPager) GetNextCursor() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextCursor
}

// Reset resets pagination to start from the beginning.
func (p *ThreadSafeCursorPager) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.nextCursor = ""
	p.hasMore = true
	p.first = true
}

// ResumePagination resumes from a specific cursor.
func (p *ThreadSafeCursorPager) ResumePagination(cursor string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.nextCursor = cursor
	p.hasMore = true
	p.first = false
}
