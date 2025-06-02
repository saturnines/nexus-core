package pagination

import (
	"fmt"
	"net/http"
)

// OffsetPager handles offset–limit pagination.
type OffsetPager struct {
	Client      HTTPDoer
	BaseReq     *http.Request
	OffsetParam string // e.g. "offset"
	SizeParam   string // e.g. "limit"
	HasMorePath string // e.g. "meta.has_more"

	offset  int
	size    int
	hasMore bool
}

// NewOffsetPager builds an OffsetPager. If initOffset less than 0 it will defaults to 0.
// If pageSize bigger or equal to  0, defaults to 100. hasMorePath can be empty.
func NewOffsetPager(
	client HTTPDoer,
	req *http.Request,
	offsetParam, sizeParam, hasMorePath string,
	initOffset, pageSize int,
) *OffsetPager {
	if initOffset < 0 {
		initOffset = 0
	}
	if pageSize <= 0 {
		pageSize = 100
	}
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

// NextRequest returns the next http.Request, or nil when done.
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

// UpdateState reads has_more (if given) or checks "data" length, then bumps offset.
func (p *OffsetPager) UpdateState(resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("pagination: unexpected status %d", resp.StatusCode)
	}
	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	if p.HasMorePath != "" {
		more, err := lookupBool(body, p.HasMorePath)
		if err != nil {
			// Missing or invalid field → assume no more pages
			p.hasMore = false
		} else {
			p.hasMore = more
		}
	} else {
		// No hasMorePath: check "data" array length directly
		if raw, exists := body["data"]; exists {
			if arr, ok := raw.([]interface{}); ok {
				p.hasMore = len(arr) > 0
			} else {
				p.hasMore = false
			}
		} else {
			p.hasMore = false
		}
	}

	if p.hasMore {
		p.offset += p.size
	}
	return nil
}
