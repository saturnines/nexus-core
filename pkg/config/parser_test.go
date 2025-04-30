package config

import (
	"testing"
)

func TestPipelineLoader_ValidMinimalConfig(t *testing.T) {
	// minimal valid config
	yamlContent := `
name: test-pipeline
source:
  type: rest
  endpoint: https://api.example.com/data
  response_mapping:
    fields:
      - name: id
        path: id
destination:
  type: postgres
  table: test_table
  schema:
    - name: id
      type: integer
      source: id
`

	// Create the loader with all validators
	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
		&PaginationValidator{},
		&AuthValidator{},
		&SchemaFieldMappingValidator{},
	)

	// parse yaml
	result, err := loader.Parse([]byte(yamlContent))
	if err != nil {
		t.Fatalf("Failed to parse valid config: %v", err)
	}

	// verify result
	pipeline, ok := result.(*Pipeline)
	if !ok {
		t.Fatal("Result is not a Pipeline")
	}

	// heck basic properties
	if pipeline.Name != "test-pipeline" {
		t.Errorf("Expected name 'test-pipeline', got '%s'", pipeline.Name)
	}

	if pipeline.Source.Type != SourceTypeREST {
		t.Errorf("Expected source type 'rest', got '%s'", pipeline.Source.Type)
	}

	if pipeline.Source.Method != "GET" {
		t.Errorf("Expected default method 'GET', got '%s'", pipeline.Source.Method)
	}
}
