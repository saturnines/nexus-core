package pagination

import (
	"fmt"
	"net/http"
	"strings"
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

// ExtractNestedValue extracts a value from nested data using a path like "data.-1.id"
func ExtractNestedValue(data interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			// case check if this is the last part and the next part is an array index
			if i < len(parts)-2 && (parts[i+1] == "-1" || isNumeric(parts[i+1])) {
				// Get the array from the current map
				if arr, ok := v[part].([]interface{}); ok {
					current = arr
					continue
				}
			}

			// Normal map access
			val, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("key %q not found", part)
			}
			current = val

		case []interface{}:
			// Handle array indexing
			if part == "-1" {
				// Get last element
				if len(v) == 0 {
					return nil, fmt.Errorf("cannot get last element of empty array")
				}
				current = v[len(v)-1]
			} else if idx, err := parseInt(part); err == nil {
				// Numeric index
				if idx < 0 || idx >= len(v) {
					return nil, fmt.Errorf("array index %d out of bounds", idx)
				}
				current = v[idx]
			} else {
				return nil, fmt.Errorf("invalid array index: %s", part)
			}

		default:
			return nil, fmt.Errorf("cannot traverse %T with key %q", current, part)
		}
	}

	return current, nil
}

// Helper to check if string is numeric
func isNumeric(s string) bool {
	_, err := parseInt(s)
	return err == nil
}

// Helper to parse int
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// NextRequest returns the next HTTP request, or nil when there are no more pages.
func (p *ThreadSafeCursorPager) NextRequest() (*http.Request, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if pagination is complete
	if !p.hasMore {
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

	// Extract next cursor value using our enhanced extraction
	nextCursorValue, err := ExtractNestedValue(body, p.nextPath)

	// Update pagination state based on next cursor
	if err != nil || nextCursorValue == nil {
		// Field missing or null - no more pages
		p.nextCursor = ""
		p.hasMore = false
	} else {
		// Convert to string and check if it's empty
		var cursorStr string
		if strVal, ok := nextCursorValue.(string); ok {
			cursorStr = strVal
		} else {
			// Try to convert to string
			cursorStr = fmt.Sprintf("%v", nextCursorValue)
		}

		// Check if cursor is empty string if so, no more pages
		if cursorStr == "" {
			p.nextCursor = ""
			p.hasMore = false
		} else {
			p.nextCursor = cursorStr
			p.hasMore = true
		}
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
