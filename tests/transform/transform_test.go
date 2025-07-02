package transform_test

import (
	"fmt"
	"testing"

	"github.com/saturnines/nexus-core/pkg/transform"
)

func TestStringTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("string", nil)
	if err != nil {
		t.Fatalf("failed to create string transformer: %v", err)
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"bool", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := transformer.Transform(tt.input)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIntTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("int", nil)
	if err != nil {
		t.Fatalf("failed to create int transformer: %v", err)
	}

	tests := []struct {
		name     string
		input    interface{}
		expected int
		wantErr  bool
	}{
		{"nil", nil, 0, false},
		{"int", 42, 42, false},
		{"float", 3.14, 3, false},
		{"string valid", "123", 123, false},
		{"string invalid", "abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := transformer.Transform(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestDateTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("date", map[string]interface{}{
		"input_format":  "Unix",
		"output_format": "RFC3339",
	})
	if err != nil {
		t.Fatalf("failed to create date transformer: %v", err)
	}

	// Test Unix timestamp to RFC3339
	unixTime := int64(1609459200) // 2021-01-01 00:00:00 UTC
	result, err := transformer.Transform(unixTime)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := "2021-01-01T00:00:00Z"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}

	// Test string date parsing
	transformer2, err := registry.Create("date", map[string]interface{}{
		"input_format":  "2006-01-02",
		"output_format": "DateTime",
	})
	if err != nil {
		t.Fatalf("failed to create date transformer: %v", err)
	}

	result2, err := transformer2.Transform("2023-12-25")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected2 := "2023-12-25 00:00:00"
	if result2 != expected2 {
		t.Errorf("expected %v, got %v", expected2, result2)
	}
}

func TestSplitTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("split", map[string]interface{}{
		"delimiter": ",",
	})
	if err != nil {
		t.Fatalf("failed to create split transformer: %v", err)
	}

	result, err := transformer.Transform("apple,banana,orange")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	arr, ok := result.([]string)
	if !ok {
		t.Errorf("expected []string, got %T", result)
	}

	if len(arr) != 3 || arr[0] != "apple" || arr[1] != "banana" || arr[2] != "orange" {
		t.Errorf("unexpected result: %v", arr)
	}
}

func TestChainTransform(t *testing.T) {
	// Can't test chain directly through registry, but we can test the sequence
	registry := transform.NewRegistry()

	// Create individual transforms
	trimmer, _ := registry.Create("trim", nil)
	lowerer, _ := registry.Create("lower", nil)
	splitter, _ := registry.Create("split", map[string]interface{}{
		"delimiter": ",",
	})

	// Apply in sequence
	input := "  APPLE,BANANA,ORANGE  "
	result := input

	// Trim
	r1, err := trimmer.Transform(result)
	if err != nil {
		t.Fatalf("trim failed: %v", err)
	}
	result = r1.(string)

	// Lower
	r2, err := lowerer.Transform(result)
	if err != nil {
		t.Fatalf("lower failed: %v", err)
	}
	result = r2.(string)

	// Split
	r3, err := splitter.Transform(result)
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}

	arr, ok := r3.([]string)
	if !ok {
		t.Errorf("expected []string, got %T", r3)
	}

	if len(arr) != 3 || arr[0] != "apple" || arr[1] != "banana" || arr[2] != "orange" {
		t.Errorf("unexpected result: %v", arr)
	}
}

func TestJoinTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("join", map[string]interface{}{
		"delimiter": ", ",
	})
	if err != nil {
		t.Fatalf("failed to create join transformer: %v", err)
	}

	input := []interface{}{"apple", "banana", "orange"}
	result, err := transformer.Transform(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := "apple, banana, orange"
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestUpperLowerTrim(t *testing.T) {
	registry := transform.NewRegistry()

	// Test upper
	upper, _ := registry.Create("upper", nil)
	result, _ := upper.Transform("hello")
	if result != "HELLO" {
		t.Errorf("upper: expected HELLO, got %v", result)
	}

	// Test lower
	lower, _ := registry.Create("lower", nil)
	result, _ = lower.Transform("HELLO")
	if result != "hello" {
		t.Errorf("lower: expected hello, got %v", result)
	}

	// Test trim
	trim, _ := registry.Create("trim", nil)
	result, _ = trim.Transform("  hello  ")
	if result != "hello" {
		t.Errorf("trim: expected 'hello', got '%v'", result)
	}
}

func TestBoolTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("bool", nil)
	if err != nil {
		t.Fatalf("failed to create bool transformer: %v", err)
	}

	tests := []struct {
		name     string
		input    interface{}
		expected bool
		wantErr  bool
	}{
		{"nil", nil, false, false},
		{"bool true", true, true, false},
		{"bool false", false, false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"int 1", 1, true, false},
		{"int 0", 0, false, false},
		{"float 1.0", 1.0, true, false},
		{"float 0.0", 0.0, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := transformer.Transform(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFloatTransform(t *testing.T) {
	registry := transform.NewRegistry()
	transformer, err := registry.Create("float", nil)
	if err != nil {
		t.Fatalf("failed to create float transformer: %v", err)
	}

	tests := []struct {
		name     string
		input    interface{}
		expected float64
		wantErr  bool
	}{
		{"nil", nil, 0.0, false},
		{"float64", 3.14, 3.14, false},
		{"int", 42, 42.0, false},
		{"string valid", "3.14", 3.14, false},
		{"string invalid", "abc", 0.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := transformer.Transform(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestRegistry(t *testing.T) {
	registry := transform.NewRegistry()

	// Test all registered types exist
	types := []string{"string", "int", "float", "bool", "date", "split", "join", "upper", "lower", "trim"}

	for _, typ := range types {
		_, err := registry.Create(typ, nil)
		if err != nil {
			t.Errorf("expected %s transformer to be registered, got error: %v", typ, err)
		}
	}

	// Test unknown transformer
	_, err := registry.Create("unknown", nil)
	if err == nil {
		t.Error("expected error for unknown transformer")
	}
}

// Test custom transformer registration
func TestCustomTransformer(t *testing.T) {
	registry := transform.NewRegistry()

	// Register it
	registry.Register("prefix", func(config map[string]interface{}) (transform.Transformer, error) {
		prefix := "PREFIX_"
		if p, ok := config["prefix"].(string); ok {
			prefix = p
		}
		return &prefixTransform{prefix: prefix}, nil
	})

	// Use it
	transformer, err := registry.Create("prefix", map[string]interface{}{
		"prefix": "ID_",
	})
	if err != nil {
		t.Fatalf("failed to create custom transformer: %v", err)
	}

	result, err := transformer.Transform("12345")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result != "ID_12345" {
		t.Errorf("expected ID_12345, got %v", result)
	}
}

// Custom transformer for testing
type prefixTransform struct {
	prefix string
}

func (t *prefixTransform) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("prefix transform requires string input")
	}
	return t.prefix + str, nil
}
