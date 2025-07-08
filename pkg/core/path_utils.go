package core

import (
	"strings"
)

// ExtractFieldEnhanced - just use the old ExtractField logic exactly
func ExtractFieldEnhanced(data interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	// Handle type assertion for non-map data
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, false
	}

	// Simple case - no dots
	if !strings.Contains(path, ".") {
		value, ok := dataMap[path]
		return value, ok
	}

	// Nested case - traverse the path
	parts := strings.Split(path, ".")
	var current interface{} = dataMap

	for _, part := range parts {
		currentMap, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}

		current, ok = currentMap[part]
		if !ok {
			return nil, false
		}
	}

	return current, true
}
