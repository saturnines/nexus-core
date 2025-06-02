package pagination

import (
	"fmt"
	"net/http"
)

// CursorPager handles cursor based pagination.
type CursorPager struct {
	Client      HTTPDoer
	BaseReq     *http.Request
	CursorParam string
	NextPath    string

	nextCursor string
	first      bool
}

// NewCursorPager builds a CursorPager. nextPath must be a non empty JSON path
// If nextPath is empty, no pagination will occur and it should stop.
func NewCursorPager(client HTTPDoer, req *http.Request, cursorParam, nextPath string) *CursorPager {
	if nextPath == "" {
		return &CursorPager{
			Client:      client,
			BaseReq:     req,
			CursorParam: cursorParam,
			NextPath:    nextPath,
			nextCursor:  "",
			first:       false, // so NextRequest() sees nextCursor=="" and returns nil
		}
	}

	return &CursorPager{
		Client:      client,
		BaseReq:     req,
		CursorParam: cursorParam,
		NextPath:    nextPath,
		first:       true,
	}
}

// NextRequest returns the next *http.Request, or nil when there are no more pages.
func (p *CursorPager) NextRequest() (*http.Request, error) {
	// If this is not the first call, and nextCursor is empty, we’re done.
	if !p.first && p.nextCursor == "" {
		return nil, nil
	}

	// Build a fresh request cloning headers and context.
	req := p.BaseReq.Clone(p.BaseReq.Context())

	// On the second+ call, add ?cursor=<nextCursor> to the query.
	if !p.first {
		q := req.URL.Query()
		q.Set(p.CursorParam, p.nextCursor)
		req.URL.RawQuery = q.Encode()
	}

	p.first = false
	return req, nil
}

// UpdateState reads the JSON response, looks up the next cursor, and stores it.
// If the JSON field at NextPath is missing, null, or empty, p.nextCursor == "" and pagination should stop.
func (p *CursorPager) UpdateState(resp *http.Response) error {
	//  Fail on HTTP errors.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("CursorPager: unexpected status %d", resp.StatusCode)
	}

	// Parse the JSON body into a map[string]interface{}.
	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	// Try to read a string from NextPath.
	cur, err := lookupString(body, p.NextPath)
	if err != nil {
		// If lookupString signals “missing field” or returns an empty string,
		// mark that the end of pagination and stop.
		p.nextCursor = ""
		return nil
	}

	// End if we get empty str
	if cur == "" {
		p.nextCursor = ""
	} else {
		p.nextCursor = cur
	}

	return nil
}
