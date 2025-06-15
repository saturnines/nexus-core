package pagination

import (
	"fmt"
	"net/http"

	"github.com/saturnines/nexus-core/pkg/errors"
)

// Creator builds a Pager or errors on bad opts.
type Creator func(HTTPDoer, *http.Request, map[string]interface{}) (Pager, error)

// DefaultRegistry maps names to creators.
var DefaultRegistry = map[string]Creator{
	"cursor": cursorCreator,
	"page":   pageCreator,
	"offset": offsetCreator,
	"link":   linkCreator,
}

func cursorCreator(c HTTPDoer, r *http.Request, opts map[string]interface{}) (Pager, error) {
	cp, err := getStringOption(opts, "cursorParam", "cursor pagination")
	if err != nil {
		return nil, err
	}
	np, err := getStringOption(opts, "nextPath", "cursor pagination")
	if err != nil {
		return nil, err
	}
	return NewCursorPager(c, r, cp, np), nil
}

func pageCreator(c HTTPDoer, r *http.Request, opts map[string]interface{}) (Pager, error) {
	pp, err := getStringOption(opts, "pageParam", "page pagination")
	if err != nil {
		return nil, err
	}
	sz, err := getStringOption(opts, "sizeParam", "page pagination")
	if err != nil {
		return nil, err
	}

	// Make hasMorePath optional since we might use totalPagesPath instead
	hm := getOptionalStringOption(opts, "hasMorePath")

	// Add support for totalPagesPath (optional)
	tp := getOptionalStringOption(opts, "totalPagesPath")

	sp, err := getIntOption(opts, "startPage", "page pagination")
	if err != nil {
		return nil, err
	}
	ps, err := getIntOption(opts, "pageSize", "page pagination")
	if err != nil {
		return nil, err
	}

	// Use the new constructor if totalPagesPath is provided
	if tp != "" {
		return NewPagePagerWithTotalPages(c, r, pp, sz, hm, tp, sp, ps), nil
	}

	// Fall back to original constructor for backward compatibility
	return NewPagePager(c, r, pp, sz, hm, sp, ps), nil
}

// Update this function in pkg/connector/api/pagination/registry.go
func offsetCreator(c HTTPDoer, r *http.Request, opts map[string]interface{}) (Pager, error) {
	op, err := getStringOption(opts, "offsetParam", "offset pagination")
	if err != nil {
		return nil, err
	}
	sz, err := getStringOption(opts, "sizeParam", "offset pagination")
	if err != nil {
		return nil, err
	}

	// Make hasMorePath optional since we might use totalCountPath instead
	hm := getOptionalStringOption(opts, "hasMorePath")

	// NEW: Add support for totalCountPath (optional)
	tc := getOptionalStringOption(opts, "totalCountPath")

	io, err := getIntOption(opts, "initOffset", "offset pagination")
	if err != nil {
		return nil, err
	}
	ps, err := getIntOption(opts, "pageSize", "offset pagination")
	if err != nil {
		return nil, err
	}

	// Use the new constructor if totalCountPath is provided (I should refactor this in the future once I'm done with e2e testing..
	if tc != "" {
		return NewOffsetPagerWithTotalCount(c, r, op, sz, hm, tc, io, ps), nil
	}

	// Fall back to original constructor for backward compatibility
	return NewOffsetPager(c, r, op, sz, hm, io, ps), nil
}

func linkCreator(c HTTPDoer, r *http.Request, opts map[string]interface{}) (Pager, error) {
	return NewLinkPager(c, r), nil
}

// Helper functions for option extraction
func getStringOption(opts map[string]interface{}, key, ctx string) (string, error) {
	v, ok := opts[key]
	if !ok {
		return "", errors.WrapError(
			fmt.Errorf("%s missing", key),
			errors.ErrConfiguration,
			ctx,
		)
	}
	s, ok := v.(string)
	if !ok {
		return "", errors.WrapError(
			fmt.Errorf("%s must be string, got %T", key, v),
			errors.ErrConfiguration,
			ctx,
		)
	}
	return s, nil
}

func getIntOption(opts map[string]interface{}, key, ctx string) (int, error) {
	v, ok := opts[key]
	if !ok {
		return 0, errors.WrapError(
			fmt.Errorf("%s missing", key),
			errors.ErrConfiguration,
			ctx,
		)
	}
	switch x := v.(type) {
	case int:
		return x, nil
	case float64:
		return int(x), nil
	default:
		return 0, errors.WrapError(
			fmt.Errorf("%s must be int, got %T", key, v),
			errors.ErrConfiguration,
			ctx,
		)
	}
}

// TODO: Add helper for optional string parameters (for improvement #4)
func getOptionalStringOption(opts map[string]interface{}, key string) string {
	if v, ok := opts[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// TODO: Add helper for optional int parameters with defaults
func getOptionalIntOption(opts map[string]interface{}, key string, defaultVal int) int {
	if v, ok := opts[key]; ok {
		switch x := v.(type) {
		case int:
			return x
		case float64:
			return int(x)
		}
	}
	return defaultVal
}
