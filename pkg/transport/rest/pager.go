package rest

import (
	"context"
	"net/http"

	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/pagination"
)

// NewPager bootstraps a REST Pager using your builder, HTTP client, and config.
func NewPager(
	ctx context.Context,
	builder *Builder,
	client *http.Client,
	pagCfg *config.Pagination,
) (pagination.Pager, error) {
	if pagCfg == nil {
		return nil, nil
	}

	// Build the very first request
	req, err := builder.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Convert your Pagination struct into the opts map
	opts := paginationConfigToOptions(pagCfg)

	// Use the default factory
	return pagination.DefaultFactory.CreatePager(
		string(pagCfg.Type),
		client,
		req,
		opts,
	)
}

// Helper function to convert config to options
func paginationConfigToOptions(p *config.Pagination) map[string]interface{} {
	opts := make(map[string]interface{})

	switch p.Type {
	case config.PaginationTypePage:
		opts["pageParam"] = p.PageParam
		opts["sizeParam"] = p.SizeParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalPagesPath"] = p.TotalPagesPath
		opts["startPage"] = 1
		opts["pageSize"] = p.PageSize
	case config.PaginationTypeOffset:
		opts["offsetParam"] = p.OffsetParam
		opts["sizeParam"] = p.LimitParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalCountPath"] = p.TotalCountPath
		opts["initOffset"] = 0
		opts["pageSize"] = p.OffsetIncrement
	case config.PaginationTypeCursor:
		opts["cursorParam"] = p.CursorParam
		opts["nextPath"] = p.CursorPath
	}

	return opts
}
