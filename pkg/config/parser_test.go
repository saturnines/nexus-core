package config

import (
	"os"
	"strings"
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

	// check basic properties
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

// Test basic config parsing with defaults ( might need to add more different ones later)
func TestPipelineLoader_DefaultValues(t *testing.T) {
	yamlContent := `
name: test-pipeline
source:
  type: rest
  endpoint: https://api.example.com/data
  # Method is not specified, should default to GET
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

	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
	)

	result, err := loader.Parse([]byte(yamlContent))
	if err != nil {
		t.Fatalf("Failed to parse valid config: %v", err)
	}

	pipeline, ok := result.(*Pipeline)
	if !ok {
		t.Fatal("Result is not a Pipeline")
	}

	// Check default method setting
	if pipeline.Source.Method != "GET" {
		t.Errorf("Expected default method 'GET', got '%s'", pipeline.Source.Method)
	}
}

// test environment variable expansion (prolly don't need to handle because it's a positive test?)
func TestPipelineLoader_EnvVarExpansion(t *testing.T) {
	// Set an environment variable for testing
	os.Setenv("TEST_API_URL", "https://api.example.com")
	defer os.Unsetenv("TEST_API_URL")

	yamlContent := `
name: test-pipeline
source:
  type: rest
  endpoint: "${TEST_API_URL}/data"
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

	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
	)

	result, err := loader.Parse([]byte(yamlContent))
	if err != nil {
		t.Fatalf("Failed to parse valid config: %v", err)
	}

	pipeline, ok := result.(*Pipeline)
	if !ok {
		t.Fatal("Result is not a Pipeline")
	}

	// Check if env vars were expanded
	expectedEndpoint := "https://api.example.com/data"
	if pipeline.Source.Endpoint != expectedEndpoint {
		t.Errorf("Environment variable not expanded correctly. Expected '%s', got '%s'",
			expectedEndpoint, pipeline.Source.Endpoint)
	}
}

// test missing fields
func TestPipelineLoader_MissingRequiredFields(t *testing.T) {
	testCases := []struct {
		name        string
		yamlContent string
		errorField  string
	}{
		{
			name: "Missing pipeline name",
			yamlContent: `
# Missing name field
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
`,
			errorField: "name",
		},
		{
			name: "Missing source type",
			yamlContent: `
name: test-pipeline
source:
  # Missing type field
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
`,
			errorField: "source.type",
		},
		{
			name: "Missing endpoint",
			yamlContent: `
name: test-pipeline
source:
  type: rest
  # Missing endpoint field
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
`,
			errorField: "source.endpoint",
		},
		{
			name: "Missing response fields",
			yamlContent: `
name: test-pipeline
source:
  type: rest
  endpoint: https://api.example.com/data
  response_mapping:
    # Missing fields
destination:
  type: postgres
  table: test_table
  schema:
    - name: id
      type: integer
      source: id
`,
			errorField: "response_mapping.fields",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loader := NewPipelineLoader(
				&EnvExpander{},
				&PipelineDefaults{},
				&RequiredFieldValidator{},
			)

			_, err := loader.Parse([]byte(tc.yamlContent))

			if err == nil {
				t.Fatalf("Expected error for missing required field, but got none")
			}

			// check if error has required fields
			if errorMsg := err.Error(); !strings.Contains(errorMsg, tc.errorField) {
				t.Errorf("Error should mention '%s', got: %s", tc.errorField, errorMsg)
			}
		})
	}
}

// test schema mapping
func TestPipelineLoader_SchemaFieldMapping(t *testing.T) {
	// Valid mapping
	validYaml := `
name: test-pipeline
source:
  type: rest
  endpoint: https://api.example.com/data
  response_mapping:
    fields:
      - name: id
        path: id
      - name: email
        path: user.email
destination:
  type: postgres
  table: test_table
  schema:
    - name: user_id
      type: integer
      source: id
      primary_key: true
    - name: email_address
      type: string
      source: email
`

	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
		&SchemaFieldMappingValidator{},
	)

	_, err := loader.Parse([]byte(validYaml))
	if err != nil {
		t.Fatalf("Failed to parse valid mapping: %v", err)
	}

	// Invalid mapping
	invalidYaml := `
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
    - name: user_id
      type: integer
      source: id
    - name: name
      type: string
      source: username  # This field doesn't exist in response_mapping
`

	_, err = loader.Parse([]byte(invalidYaml))
	if err == nil {
		t.Fatal("Expected validation error for invalid field mapping, but got none")
	}

	// Check that the error mentions the non-existent field
	if errorMsg := err.Error(); !strings.Contains(errorMsg, "username") {
		t.Errorf("Error should mention non-existent field 'username', got: %s", errorMsg)
	}
}

// test invalid YAML syntax
func TestPipelineLoader_InvalidYAML(t *testing.T) {
	invalidYaml := `
name: test-pipeline
source:
  type: rest
  endpoint: https://api.example.com/data
  # Invalid YAML indentation and formatting
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

	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
	)

	_, err := loader.Parse([]byte(invalidYaml))
	if err == nil {
		t.Fatal("Expected error for invalid YAML, but got none")
	}

	// Error should say YAML parsing issues
	if errorMsg := err.Error(); !strings.Contains(errorMsg, "parse YAML") {
		t.Errorf("Error should mention YAML parsing, got: %s", errorMsg)
	}
}

// test multiple validators together
func TestPipelineLoader_MultipleValidators(t *testing.T) {
	// config with multiple issues
	yamlContent := `
# Missing name field
source:
  type: rest
  # Missing endpoint
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
    - name: email
      type: string
      source: email  # This field doesn't exist in response_mapping
`

	loader := NewPipelineLoader(
		&EnvExpander{},
		&PipelineDefaults{},
		&RequiredFieldValidator{},
		&SchemaFieldMappingValidator{},
	)

	_, err := loader.Parse([]byte(yamlContent))
	if err == nil {
		t.Fatal("Expected validation errors, but got none")
	}

	// Check that the error mentions multiple issues
	errorMsg := err.Error()

	if !strings.Contains(errorMsg, "name") {
		t.Error("Error should mention missing 'name' field")
	}

	if !strings.Contains(errorMsg, "source.endpoint") {
		t.Error("Error should mention missing 'source.endpoint' field")
	}

	if !strings.Contains(errorMsg, "email") {
		t.Error("Error should mention non-existent field 'email'")
	}
}
