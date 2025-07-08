package core

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/saturnines/nexus-core/pkg/config"
)

// GraphQLExtractor implements Extractor for GraphQL sources.
type GraphQLExtractor struct {
	rootPath string
	fields   []config.Field
}

// NewGraphQLExtractor initialises a GraphQLExtractor.
func NewGraphQLExtractor(g *config.GraphQLSource) *GraphQLExtractor {
	var root string
	rp := g.ResponseMapping.RootPath

	switch {
	case rp == "", rp == "data":
		root = "data"
	case strings.HasPrefix(rp, "data."):
		root = rp
	default:
		root = "data." + rp
	}

	return &GraphQLExtractor{
		rootPath: root,
		fields:   g.ResponseMapping.Fields,
	}
}

// Items extracts the slice of items from the GraphQL response body.
func (e *GraphQLExtractor) Items(b []byte) ([]interface{}, error) {
	var raw interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("failed to decode response JSON: %w", err)
	}

	// Navigate to the configured root
	node, ok := ExtractFieldEnhanced(raw, e.rootPath)
	if !ok || node == nil {
		return nil, fmt.Errorf("root path '%s' not found", e.rootPath)
	}

	// Return array or single-object fallback
	switch arr := node.(type) {
	case []interface{}:
		return arr, nil
	case map[string]interface{}:
		return []interface{}{arr}, nil
	default:
		return nil, fmt.Errorf("root path '%s' does not point to array or object", e.rootPath)
	}
}

// Map applies the field mappings to a single item.
func (e *GraphQLExtractor) Map(item interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(e.fields))
	for _, f := range e.fields {
		if v, ok := ExtractFieldEnhanced(item, f.Path); ok && v != nil {
			m[f.Name] = v
		} else if f.DefaultValue != nil {
			m[f.Name] = f.DefaultValue
		}
	}
	return m, nil
}
