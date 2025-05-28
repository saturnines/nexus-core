package pagination

import (
	"fmt"
	"net/http"
)

// OffsetPager for offset-based APIs.
type OffsetPager struct {
	Client      HTTPDoer
	BaseReq     *http.Request
	OffsetParam string
	SizeParam   string
	HasMorePath string

	offset  int
	size    int
	hasMore bool
}

// NewOffsetPager builds an OffsetPager.
func NewOffsetPager(client HTTPDoer, req *http.Request, offsetParam, sizeParam, hasMorePath string, initOffset, pageSize int) *OffsetPager {
	return &OffsetPager{
		Client:      client,
		BaseReq:     req,
		OffsetParam: offsetParam,
		SizeParam:   sizeParam,
		HasMorePath: hasMorePath,
		offset:      initOffset,
		size:        pageSize,
		hasMore:     true,
	}
}

// NextRequest returns the next request or nil when done.
func (p *OffsetPager) NextRequest() (*http.Request, error) {
	if !p.hasMore {
		return nil, nil
	}
	req := p.BaseReq.Clone(p.BaseReq.Context())
	q := req.URL.Query()
	q.Set(p.OffsetParam, fmt.Sprint(p.offset))
	q.Set(p.SizeParam, fmt.Sprint(p.size))
	req.URL.RawQuery = q.Encode()
	return req, nil
}

// UpdateState reads has_more and bumps offset.
func (p *OffsetPager) UpdateState(resp *http.Response) error {
	body, err := parseBody(resp)
	if err != nil {
		return err
	}
	more, err := lookupBool(body, p.HasMorePath)
	if err != nil {
		return err
	}
	p.hasMore = more
	if more {
		p.offset += p.size
	}
	return nil
}
