package pagination

import (
	"fmt"
	"github.com/saturnines/nexus-core/pkg/errors"
	"net/http"
	"strings"
)

// PagePager for "page + page_size + has_more" pagination.
type PagePager struct {
	Client         HTTPDoer
	BaseReq        *http.Request
	PageParam      string // e.g. "page"
	SizeParam      string // e.g. "page_size"
	HasMorePath    string // e.g. "meta.has_more"
	TotalPagesPath string // e.g. "meta.total_pages" - NEW FIELD

	page       int
	size       int
	first      bool
	hasMore    bool
	totalPages int // NEW FIELD - cached total pages from response
}

// NewPagePager builds a PagePager. If startPage less than 1, defaults to 1.
// If pageSize more than 0, defaults to 100.
// should never returns a "has_more" boolean should stop if the data is empty? (need to test)
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
		totalPages:  0, // Will be set from response
	}
}

// NewPagePagerWithTotalPages builds a PagePager with total pages support.
func NewPagePagerWithTotalPages(
	client HTTPDoer,
	req *http.Request,
	pageParam, sizeParam, hasMorePath, totalPagesPath string,
	startPage, pageSize int,
) *PagePager {
	if startPage < 1 {
		startPage = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	return &PagePager{
		Client:         client,
		BaseReq:        req,
		PageParam:      pageParam,
		SizeParam:      sizeParam,
		HasMorePath:    hasMorePath,
		TotalPagesPath: totalPagesPath, // NEW
		page:           startPage,
		size:           pageSize,
		first:          true,
		hasMore:        true,
		totalPages:     0, // Will be set from response
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

// UpdateState inspects the JSON body for pagination control fields.
// Priority: 1) total_pages, 2) has_more, 3) data array length
func (p *PagePager) UpdateState(resp *http.Response) error {
	// 1) Check HTTP status.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.WrapError(
			fmt.Errorf("pagination: unexpected status %d", resp.StatusCode),
			errors.ErrPagination,
			"update page state",
		)
	}

	// Unmarshal JSON.
	body, err := parseBody(resp)
	if err != nil {
		return err
	}

	// NCheck total_pages field
	if p.TotalPagesPath != "" {
		totalPages, err := lookupInt(body, p.TotalPagesPath)
		if err != nil {
			// If total_pages field is missing/invalid, fall back to other methods
			// Don't return error, just continue to next method
		} else {
			p.totalPages = totalPages
			p.hasMore = p.page < p.totalPages
			return nil
		}
	}

	// Priority 2 - Check has_more field
	if p.HasMorePath != "" {
		more, err := lookupBool(body, p.HasMorePath)
		if err != nil {
			// Missing or invalid field safely degrade and assume no more pages.
			p.hasMore = false
		} else {
			p.hasMore = more
		}
		return nil
	}

	// Priority 3 - Fallback to checking data array length (existing logic)
	raw, exists := body["data"]
	if !exists {
		p.hasMore = false
	} else if arr, ok := raw.([]interface{}); ok {
		p.hasMore = len(arr) > 0
	} else {
		p.hasMore = false
	}

	return nil
}

// lookupInt helper function to extract integer from nested JSON path
func lookupInt(body map[string]interface{}, path string) (int, error) {
	parts := strings.Split(path, ".")
	var cur interface{} = body
	for _, key := range parts {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return 0, errors.WrapError(
				fmt.Errorf("lookupInt: %q is not an object", key),
				errors.ErrExtraction,
				"traverse object",
			)
		}
		cur, ok = m[key]
		if !ok {
			return 0, errors.WrapError(
				fmt.Errorf("lookupInt: missing field %q", key),
				errors.ErrExtraction,
				"find field",
			)
		}
	}

	// Handle different number types from JSON
	switch v := cur.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int64:
		return int(v), nil
	default:
		return 0, errors.WrapError(
			fmt.Errorf("lookupInt: field %q is not a number, got %T", path, v),
			errors.ErrExtraction,
			"convert to integer",
		)
	}
}
