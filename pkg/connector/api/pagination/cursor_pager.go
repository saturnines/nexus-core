package pagination

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// CursorPager handles cursor based pagination.
type CursorPager struct {
	mu          sync.Mutex
	Client      HTTPDoer
	BaseReq     *http.Request
	CursorParam string
	NextPath    string

	nextCursor string
	first      bool
}

// NewCursorPager builds a CursorPager.
func NewCursorPager(client HTTPDoer, req *http.Request, cursorParam, nextPath string) *CursorPager {
	return &CursorPager{
		Client:      client,
		BaseReq:     req,
		CursorParam: cursorParam,
		NextPath:    nextPath,
		first:       true,
	}
}

// NextRequest returns the next request or nil when done.
func (p *CursorPager) NextRequest() (*http.Request, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.first && p.nextCursor == "" {
		return nil, nil
	}

	req := p.BaseReq.Clone(p.BaseReq.Context())
	if !p.first {
		q := req.URL.Query()
		q.Set(p.CursorParam, p.nextCursor)
		req.URL.RawQuery = q.Encode()
	}
	p.first = false
	return req, nil
}

// UpdateState reads the next cursor with edge case handling.
func (p *CursorPager) UpdateState(resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("CursorPager: bad status %d", resp.StatusCode)
	}

	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	cur, err := lookupString(body, p.NextPath)
	if err != nil {
		if strings.Contains(err.Error(), "missing field") {
			// last page omits cursor, this is thread-safe technically but I think it might be a bit overkill.
			p.mu.Lock()
			p.nextCursor = ""
			p.mu.Unlock()
			return nil
		}
		return err
	}

	p.mu.Lock()
	p.nextCursor = cur
	p.mu.Unlock()
	return nil
}
