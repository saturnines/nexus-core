package core

import (
	"encoding/json"
	"fmt"

	"github.com/saturnines/nexus-core/pkg/config"
)

type GraphQLExtractor struct {
	rootPath string
	fields   []config.Field
}

func NewGraphQLExtractor(g *config.GraphQLSource) *GraphQLExtractor {
	return &GraphQLExtractor{
		rootPath: g.ResponseMapping.RootPath,
		fields:   g.ResponseMapping.Fields,
	}
}

func (e *GraphQLExtractor) Items(raw []byte) ([]interface{}, error) {
	var resp map[string]interface{}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	dataRoot := resp["data"]
	if dataRoot == nil {
		return []interface{}{}, nil
	}
	root, ok := ExtractFieldEnhanced(dataRoot, e.rootPath)
	if !ok {
		return nil, fmt.Errorf("root path '%s' not found", e.rootPath)
	}
	switch arr := root.(type) {
	case []interface{}:
		return arr, nil
	case map[string]interface{}:
		return []interface{}{arr}, nil
	default:
		return nil, fmt.Errorf("unexpected GraphQL root type: %T", root)
	}
}

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
