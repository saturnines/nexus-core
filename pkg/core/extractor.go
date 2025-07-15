package core

import (
	"fmt"
	"strconv"
	"strings"
)

// ExtractFieldEnhanced extracts a field from data using an enhanced path syntax
// Supports:
// - Nested fields: "user.name"
// - Array indices: "items[0]", "items[-1]" (negative for last)
// - Array wildcards: "items[*].name"
// - Complex paths: "data.users[*].addresses[0].city"
func ExtractFieldEnhanced(data interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	// Parse the path into segments
	segments, err := parsePath(path)
	if err != nil {
		return nil, false
	}

	return traversePath(data, segments)
}

// PathSegment represents a single segment in a path
type PathSegment struct {
	Field string
	Type  SegmentType
	Index int // For array indices
}

type SegmentType int

const (
	FieldSegment  SegmentType = iota // Regular field access
	ArrayIndex                       // Specific array index [0], [-1]
	ArrayWildcard                    // Array wildcard [*]
)

// parsePath converts a string path into structured segments
func parsePath(path string) ([]PathSegment, error) {
	var segments []PathSegment

	// Handle paths that start with array notation (e.g., "[0].field")
	if strings.HasPrefix(path, "[") {
		path = "." + path
	}

	parts := strings.Split(path, ".")

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check for array notation
		if idx := strings.Index(part, "["); idx != -1 {
			// Field name before bracket
			if idx > 0 {
				fieldName := part[:idx]
				segments = append(segments, PathSegment{
					Field: fieldName,
					Type:  FieldSegment,
				})
			}

			// Parse array indices
			remaining := part[idx:]
			for len(remaining) > 0 {
				if !strings.HasPrefix(remaining, "[") {
					return nil, fmt.Errorf("invalid array notation in path: %s", part)
				}

				endIdx := strings.Index(remaining, "]")
				if endIdx == -1 {
					return nil, fmt.Errorf("unclosed bracket in path: %s", part)
				}

				indexStr := remaining[1:endIdx]

				// Handle wildcard
				if indexStr == "*" {
					segments = append(segments, PathSegment{
						Type: ArrayWildcard,
					})
				} else {
					// Parse numeric index
					index, err := strconv.Atoi(indexStr)
					if err != nil {
						return nil, fmt.Errorf("invalid array index: %s", indexStr)
					}
					segments = append(segments, PathSegment{
						Type:  ArrayIndex,
						Index: index,
					})
				}

				// Move past this bracket pair
				remaining = remaining[endIdx+1:]

				// Handle chained array access like [0][1]
				if len(remaining) > 0 && !strings.HasPrefix(remaining, "[") {
					// There's a field after the bracket
					if dotIdx := strings.Index(remaining, "."); dotIdx != -1 {
						// Skip to next part
						break
					} else {
						// This is an error - text after ] without .
						return nil, fmt.Errorf("invalid syntax after bracket: %s", remaining)
					}
				}
			}
		} else {
			// Simple field
			segments = append(segments, PathSegment{
				Field: part,
				Type:  FieldSegment,
			})
		}
	}

	return segments, nil
}

// traversePath walks through data following the path segments
func traversePath(data interface{}, segments []PathSegment) (interface{}, bool) {
	current := data

	for i, segment := range segments {
		switch segment.Type {
		case FieldSegment:
			// Handle map field access
			switch v := current.(type) {
			case map[string]interface{}:
				val, ok := v[segment.Field]
				if !ok {
					return nil, false
				}
				current = val

			case []interface{}:
				// If we're trying to access a field on an array,
				// and this is followed by a wildcard, we might be doing
				// something like items[*].name
				if i+1 < len(segments) && segments[i+1].Type == ArrayWildcard {
					// Skip wildcard will handle
					continue
				}
				return nil, false

			default:
				return nil, false
			}

		case ArrayIndex:
			// Handle array index access
			arr, ok := current.([]interface{})
			if !ok {
				return nil, false
			}

			index := segment.Index
			// Handle negative indices
			if index < 0 {
				index = len(arr) + index
			}

			if index < 0 || index >= len(arr) {
				return nil, false
			}

			current = arr[index]

		case ArrayWildcard:
			// Handle array wildcard
			arr, ok := current.([]interface{})
			if !ok {
				return nil, false
			}

			// If this is the last segment just return the whole array
			if i == len(segments)-1 {
				return arr, true
			}

			// Otherwise collect results from remaining path on each element
			var results []interface{}
			remainingSegments := segments[i+1:]

			for _, elem := range arr {
				if result, ok := traversePath(elem, remainingSegments); ok {
					// Handle nested wildcards - flatten arrays
					if resultArr, isArr := result.([]interface{}); isArr {
						results = append(results, resultArr...)
					} else {
						results = append(results, result)
					}
				}
			}

			return results, len(results) > 0
		}
	}

	return current, true
}

// ExtractFieldsMulti Helper function to extract multiple fields with array support
func ExtractFieldsMulti(data interface{}, path string) ([]interface{}, error) {
	result, ok := ExtractFieldEnhanced(data, path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}

	// If result is already an array, return it
	if arr, ok := result.([]interface{}); ok {
		return arr, nil
	}

	// Otherwise wrap single result in array
	return []interface{}{result}, nil
}

// PathBuilder helps construct paths programmatically
type PathBuilder struct {
	segments []string
}

// NewPathBuilder creates a new path builder
func NewPathBuilder() *PathBuilder {
	return &PathBuilder{
		segments: make([]string, 0),
	}
}

// Field adds a field segment
func (pb *PathBuilder) Field(name string) *PathBuilder {
	pb.segments = append(pb.segments, name)
	return pb
}

// Index adds an array index segment
func (pb *PathBuilder) Index(idx int) *PathBuilder {
	if len(pb.segments) == 0 {
		pb.segments = append(pb.segments, fmt.Sprintf("[%d]", idx))
	} else {
		lastIdx := len(pb.segments) - 1
		pb.segments[lastIdx] = pb.segments[lastIdx] + fmt.Sprintf("[%d]", idx)
	}
	return pb
}

// Wildcard adds an array wildcard segment
func (pb *PathBuilder) Wildcard() *PathBuilder {
	if len(pb.segments) == 0 {
		pb.segments = append(pb.segments, "[*]")
	} else {
		lastIdx := len(pb.segments) - 1
		pb.segments[lastIdx] = pb.segments[lastIdx] + "[*]"
	}
	return pb
}

// Build returns the constructed path
func (pb *PathBuilder) Build() string {
	return strings.Join(pb.segments, ".")
}
