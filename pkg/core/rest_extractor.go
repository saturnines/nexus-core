package core

import (
	"encoding/json"
	"fmt"

	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors"
)

type RestExtractor struct {
	rootPath string
	fields   []config.Field
}

func NewRestExtractor(m config.ResponseMapping) *RestExtractor {
	return &RestExtractor{rootPath: m.RootPath, fields: m.Fields}
}

func (e *RestExtractor) Items(raw []byte) ([]interface{}, error) {
	var responseData interface{}
	if err := json.Unmarshal(raw, &responseData); err != nil {
		return nil, errors.WrapError(err, errors.ErrHTTPResponse, "failed to decode response JSON")
	}

	// Handle null response
	if responseData == nil {
		return []interface{}{}, nil
	}

	// Handle array at root level (like JSONPlaceholder)
	if arr, ok := responseData.([]interface{}); ok {
		return arr, nil
	}

	// Handle object with nested arrays
	if objMap, ok := responseData.(map[string]interface{}); ok {
		return e.extractItems(objMap)
	}

	return nil, fmt.Errorf("unexpected response format: %T", responseData)
}

func (e *RestExtractor) extractItems(responseData map[string]interface{}) ([]interface{}, error) {
	rp := e.rootPath
	if rp == "" {
		if items, ok := responseData["items"].([]interface{}); ok {
			return items, nil
		}
		if data, ok := responseData["data"].([]interface{}); ok {
			return data, nil
		}
		return []interface{}{responseData}, nil
	}

	root, ok := ExtractFieldEnhanced(responseData, rp)
	if !ok {
		return nil, fmt.Errorf("root path '%s' not found", rp)
	}

	items, ok := root.([]interface{})
	if !ok {
		return nil, fmt.Errorf("root path '%s' is not an array", rp)
	}

	return items, nil
}

func (e *RestExtractor) Map(item interface{}) (map[string]interface{}, error) {
	// Don't validate here - let connector handle it
	mapped := make(map[string]interface{})
	for _, field := range e.fields {
		value, ok := ExtractFieldEnhanced(item, field.Path)

		// Check if field is missing OR null
		if !ok || value == nil {
			if field.DefaultValue != nil {
				mapped[field.Name] = field.DefaultValue
			}
			continue
		}

		mapped[field.Name] = value
	}
	return mapped, nil
}
