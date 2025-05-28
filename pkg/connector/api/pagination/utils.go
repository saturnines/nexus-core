package pagination

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// if for some reason an api has unexpected pagination handling just add it here.

// parseBody reads and parses JSON into a generic map.
func parseBody(resp *http.Response) (map[string]interface{}, error) {
	defer resp.Body.Close()
	var data map[string]interface{}
	err := json.NewDecoder(resp.Body).Decode(&data)
	return data, err
}

// lookupString drills into a nested map by a dotted path and returns a string.
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
			return false, fmt.Errorf("lookupBool: %q is not an object", key)
		}
		cur, ok = m[key]
		if !ok {
			return false, fmt.Errorf("lookupBool: missing field %q", key)
		}
	}
	b, ok := cur.(bool)
	if !ok {
		return false, fmt.Errorf("lookupBool: field %q is not a bool", path)
	}
	return b, nil
}
