package pagination

import (
	"fmt"
	"net/http"
)

// PagePager for “page + page_size + has_more” pagination.
type PagePager struct {
	Client      HTTPDoer
	BaseReq     *http.Request
	PageParam   string // e.g. "page"
	SizeParam   string // e.g. "page_size"
	HasMorePath string // e.g. "meta.has_more"

	page    int
	size    int
	first   bool
	hasMore bool
}

// NewPagePager builds a PagePager. If startPage less than 1, defaults to 1.
// If pageSize more than 0, defaults to 100.
// should never returns a “has_more” boolean should stop if the data is empty? (need to test)
func NewPagePager(
	client HTTPDoer,
	req *http.Request,
	pageParam, sizeParam, hasMorePath string,
	startPage, pageSize int,
) *PagePager {
	if startPage < 1 {
		startPage = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	return &PagePager{
		Client:      client,
		BaseReq:     req,
		PageParam:   pageParam,
		SizeParam:   sizeParam,
		HasMorePath: hasMorePath,
		page:        startPage,
		size:        pageSize,
		first:       true,
		hasMore:     true,
	}
}

// NextRequest returns the next *http.Request, or nil when done.
func (p *PagePager) NextRequest() (*http.Request, error) {
	// If not first call and hasMore==false, end pagination.
	if !p.first && !p.hasMore {
		return nil, nil
	}

	//  bump page on second call.
	if !p.first {
		p.page++
	}

	// Build a fresh request with new query params.
	req := p.BaseReq.Clone(p.BaseReq.Context())
	q := req.URL.Query()
	q.Set(p.PageParam, fmt.Sprint(p.page))
	q.Set(p.SizeParam, fmt.Sprint(p.size))
	req.URL.RawQuery = q.Encode()

	p.first = false
	return req, nil
}

// UpdateState inspects the JSON body for “has_more”. If hasMorePath is empty,
func (p *PagePager) UpdateState(resp *http.Response) error {
	// 1) Check HTTP status.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("pagination: unexpected status %d", resp.StatusCode)
	}

	// Unmarshal JSON.
	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	//  If api has a hasMorePath use lookupBool.
	if p.HasMorePath != "" {
		more, err := lookupBool(body, p.HasMorePath)
		if err != nil {
			// Missing or invalid field safely degrade and assume no  pages.
			p.hasMore = false
		} else {
			p.hasMore = more
		}
	} else {
		//No hasMorePath just fallback to checking data array length.
		raw, exists := body["data"]
		if !exists {
			p.hasMore = false
		} else if arr, ok := raw.([]interface{}); ok {
			p.hasMore = len(arr) > 0
		} else {
			p.hasMore = false
		}
	}

	return nil
}
