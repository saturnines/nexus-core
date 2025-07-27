package pagination

import (
	"encoding/json"
	"fmt"
	"github.com/saturnines/nexus-core/pkg/errors"
	"net/http"
	"strings"
)

// if for some reason an api has unexpected pagination handling just add it here.
// parseBody reads and parses JSON into a generic map.
func parseBody(resp *http.Response) (map[string]interface{}, error) {
	defer resp.Body.Close()

	var raw interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, errors.WrapError(
			fmt.Errorf("unexpected response type: %T", raw),
			errors.ErrHTTPResponse,
			"parse response body",
		)
	}

	// If it's already an object, return it
	if obj, ok := raw.(map[string]interface{}); ok {
		return obj, nil
	}

	// If it's an array, wrap it in a data field
	if arr, ok := raw.([]interface{}); ok {
		return map[string]interface{}{
			"data": arr,
		}, nil
	}

	return nil, fmt.Errorf("unexpected response type: %T", raw)
}

// This isn't used might be used in the future I guess
func lookupString(body map[string]interface{}, path string) (string, error) {
	parts := strings.Split(path, ".")
	var cur interface{} = body
	for _, key := range parts {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("lookupString: %q is not an object", key)
		}
		cur, ok = m[key]
		if !ok {
			return "", fmt.Errorf("lookupString: missing field %q", key)
		}
	}

	// Handle null values
	if cur == nil {
		return "", nil // Treat null as empty string should stop pagination
	}

	s, ok := cur.(string)
	if !ok {
		return "", fmt.Errorf("lookupString: field %q is not a string", path)
	}
	return s, nil
}

// lookupBool drills into a nested map by a dotted path and returns a bool.
func lookupBool(body map[string]interface{}, path string) (bool, error) {
	parts := strings.Split(path, ".")
	var cur interface{} = body
	for _, key := range parts {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return false, errors.WrapError(
				fmt.Errorf("lookupBool: %q is not an object", key),
				errors.ErrExtraction,
				"traverse object",
			)
		}
		cur, ok = m[key]
		if !ok {
			return false, errors.WrapError(
				fmt.Errorf("lookupBool: missing field %q", key),
				errors.ErrExtraction,
				"find field",
			)
		}
	}
	b, ok := cur.(bool)
	if !ok {
		return false, errors.WrapError(
			fmt.Errorf("lookupBool: field %q is not a bool", path),
			errors.ErrExtraction,
			"convert to boolean",
		)
	}
	return b, nil
}
