package pagination

import (
	"fmt"
	"net/http"
)

// PagePager for page-number APIs.
type PagePager struct {
	Client      HTTPDoer
	BaseReq     *http.Request
	PageParam   string
	SizeParam   string
	HasMorePath string

	page    int
	size    int
	first   bool
	hasMore bool
}

// NewPagePager builds a PagePager.
func NewPagePager(client HTTPDoer, req *http.Request, pageParam, sizeParam, hasMorePath string, startPage, pageSize int) *PagePager {
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

// NextRequest returns the next request or nil when done.
func (p *PagePager) NextRequest() (*http.Request, error) {
	if !p.first && !p.hasMore {
		return nil, nil
	}
	req := p.BaseReq.Clone(p.BaseReq.Context())
	if !p.first {
		p.page++
	}
	q := req.URL.Query()
	q.Set(p.PageParam, fmt.Sprint(p.page))
	q.Set(p.SizeParam, fmt.Sprint(p.size))
	req.URL.RawQuery = q.Encode()
	p.first = false
	return req, nil
}

// UpdateState reads has_more to drive paging.
func (p *PagePager) UpdateState(resp *http.Response) error {
	body, err := parseBody(resp)
	if err != nil {
		return err
	}
	more, err := lookupBool(body, p.HasMorePath)
	if err != nil {
		return err
	}
	p.hasMore = more
	return nil
}
