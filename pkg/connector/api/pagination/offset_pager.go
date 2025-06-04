package pagination

import (
	"fmt"
	"net/http"
)

// OffsetPager handles offset–limit pagination.
type OffsetPager struct {
	Client         HTTPDoer
	BaseReq        *http.Request
	OffsetParam    string // e.g. "offset"
	SizeParam      string // e.g. "limit"
	HasMorePath    string // e.g. "meta.has_more"
	TotalCountPath string // e.g. "meta.total_count" - NEW FIELD

	offset     int
	size       int
	hasMore    bool
	totalCount int // NEW FIELD - cached total count from response
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
		totalCount:  -1, // -1 means not set yet
	}
}

// NewOffsetPagerWithTotalCount builds an OffsetPager with total count support.
// NEW CONSTRUCTOR for total_count support
func NewOffsetPagerWithTotalCount(
	client HTTPDoer,
	req *http.Request,
	offsetParam, sizeParam, hasMorePath, totalCountPath string,
	initOffset, pageSize int,
) *OffsetPager {
	if initOffset < 0 {
		initOffset = 0
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	return &OffsetPager{
		Client:         client,
		BaseReq:        req,
		OffsetParam:    offsetParam,
		SizeParam:      sizeParam,
		HasMorePath:    hasMorePath,
		TotalCountPath: totalCountPath, // NEW
		offset:         initOffset,
		size:           pageSize,
		hasMore:        true,
		totalCount:     -1, // -1 means not set yet
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

// UpdateState reads response and determines if more pages exist.
// Priority: 1) total_count, 2) has_more, 3) data array length
func (p *OffsetPager) UpdateState(resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("pagination: unexpected status %d", resp.StatusCode)
	}

	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	//  Check total_count field
	if p.TotalCountPath != "" {
		totalCount, err := lookupInt(body, p.TotalCountPath)
		if err != nil {
			// If total_count field is missing/invalid, fall back to other methods
			// Don't return error, just continue to next method
		} else {
			p.totalCount = totalCount
			// Calculate next offset
			nextOffset := p.offset + p.size
			p.hasMore = nextOffset < p.totalCount

			// Update offset for next request if there are more pages
			if p.hasMore {
				p.offset = nextOffset
			}
			return nil
		}
	}

	// Priority 2 - Check has_more field (existing logic)
	if p.HasMorePath != "" {
		more, err := lookupBool(body, p.HasMorePath)
		if err != nil {
			// Missing or invalid field → assume no more pages
			p.hasMore = false
		} else {
			p.hasMore = more
		}

		// Update offset for next request if there are more pages
		if p.hasMore {
			p.offset += p.size
		}
		return nil
	}

	// Fallback to checking data array length
	if raw, exists := body["data"]; exists {
		if arr, ok := raw.([]interface{}); ok {
			p.hasMore = len(arr) > 0
		} else {
			p.hasMore = false
		}
	} else {
		p.hasMore = false
	}

	// Update offset for next request if there are more pages
	if p.hasMore {
		p.offset += p.size
	}
	return nil
}
