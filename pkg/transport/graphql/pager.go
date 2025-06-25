package graphql

import (
	"context"
	"encoding/json"
	"net/http"
)

// GraphQLPager drives cursor paging in GraphQL.
// It mutates vars["after"] (or other key) each round.
type GraphQLPager struct {
	client      *Client
	builder     *Builder
	cursorKey   string   // e.g. "after"
	nextPath    []string // JSON path to "pageInfo.endCursor"
	hasNextPath []string // JSON path to "pageInfo.hasNextPage"
	hasNext     bool
}

// NewPager sets up GraphQLPager if cfg.Type=="cursor".
func NewPager(
	ctx context.Context,
	builder *Builder,
	client *Client,
	cursorKey string,
	nextPath, hasNextPath []string,
) (*GraphQLPager, error) {
	// initial build to validate
	req, err := builder.Build(ctx)
	if err != nil {
		return nil, err
	}
	// run once to prime pageInfo (optional)
	_, err = client.Execute(req)
	if err != nil {
		return nil, err
	}
	return &GraphQLPager{
		client:      client,
		builder:     builder,
		cursorKey:   cursorKey,
		nextPath:    nextPath,
		hasNextPath: hasNextPath,
		hasNext:     true,
	}, nil
}

// NextRequest returns the next *http.Request or nil when done.
func (p *GraphQLPager) NextRequest(ctx context.Context) (*http.Request, error) {
	if !p.hasNext {
		return nil, nil
	}
	return p.builder.Build(ctx)
}

// UpdateState inspects resp JSON to pull out new cursor & hasNext.
func (p *GraphQLPager) UpdateState(resp *http.Response) error {
	defer resp.Body.Close()
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	// traverse to pageInfo
	pi := traverse(data, append(p.nextPath, p.hasNextPath...)...)
	// extract cursor & flag; pseudo‐code
	// p.builder.Variables[p.cursorKey] = newCursor
	// p.hasNext = newHasNext
	return nil
}

// traverse is a helper to dig into nested maps.
func traverse(m map[string]interface{}, path ...string) interface{} {
	cur := interface{}(m)
	for _, key := range path {
		if nm, ok := cur.(map[string]interface{}); ok {
			cur = nm[key]
		} else {
			return nil
		}
	}
	return cur
}
