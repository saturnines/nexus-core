package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

// ConfigLoader defines the interface for loading configs
type ConfigLoader interface {
	Load(path string) (interface{}, error)
	Parse(data []byte) (interface{}, error)
}

type ValidationError struct {
	Field   string
	Message string
}

type Validator interface {
	Validate(config interface{}) []ValidationError
}

// Returns the string representation of validation error
func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// DefaultValueSetter Handles the interface for setting default values
type DefaultValueSetter interface {
	SetDefaults(config interface{})
}

// VariableExpander defines the interface for expanding variables
type VariableExpander interface {
	Expand(data []byte) []byte
}

// EnvExpander implements VariableExpander using environment variables
type EnvExpander struct{}

// Expand expands environment variables with the given data
func (e *EnvExpander) Expand(data []byte) []byte {
	expanded := os.Expand(string(data), os.Getenv)
	return []byte(expanded)
}

// PipelineLoader uses ConfigLoader for Pipeline configurations
type PipelineLoader struct {
	expander      VariableExpander
	validators    []Validator
	defaultSetter DefaultValueSetter
}

// NewPipelineLoader creates a new PipelineLoader with the given components
func NewPipelineLoader(
	expander VariableExpander,
	defaultSetter DefaultValueSetter,
	validators ...Validator,
) *PipelineLoader {
	return &PipelineLoader{
		expander:      expander,
		validators:    validators,
		defaultSetter: defaultSetter,
	}
}

// Load a new pipeline config from YAML file
func (l *PipelineLoader) Load(path string) (interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return l.Parse(data)
}

// Parse parses a yaml config
func (l *PipelineLoader) Parse(data []byte) (interface{}, error) {
	// Expand variables if an expander is configured
	if l.expander != nil {
		data = l.expander.Expand(data)
	}

	// Unmarshal YAML data into Pipeline struct
	var pipeline Pipeline
	if err := yaml.Unmarshal(data, &pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Set default values if a default setter is configured
	if l.defaultSetter != nil {
		l.defaultSetter.SetDefaults(&pipeline)
	}

	// Validate the pipeline configuration
	var allErrors []ValidationError
	for _, validator := range l.validators {
		errors := validator.Validate(&pipeline)
		allErrors = append(allErrors, errors...)
	}

	// Return any validation errors if there are any
	if len(allErrors) > 0 {
		return nil, fmt.Errorf("validation errors: %v", allErrors)
	}

	return &pipeline, nil
}

// PipelineDefaults implements DefaultValueSetter for Pipeline
type PipelineDefaults struct{}

// SetDefaults sets default values for Pipeline
func (d *PipelineDefaults) SetDefaults(config interface{}) {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return
	}

	// Set default HTTP method if not specified
	if pipeline.Source.Method == "" {
		pipeline.Source.Method = "GET"
	}

	// Todo: Maybe add some other defaults if needed
}

// RequiredFieldValidator validates required fields for the API
type RequiredFieldValidator struct{}

// Validate checks that all required fields are present from the API
func (v *RequiredFieldValidator) Validate(config interface{}) []ValidationError {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return []ValidationError{{Field: "config", Message: "not a Pipeline"}}
	}

	var errors []ValidationError

	// Check required fields
	if pipeline.Name == "" {
		errors = append(errors, ValidationError{Field: "name", Message: "is required"})
	}

	if string(pipeline.Source.Type) == "" {
		errors = append(errors, ValidationError{Field: "source.type", Message: "is required"})
	}

	if pipeline.Source.Endpoint == "" {
		errors = append(errors, ValidationError{Field: "source.endpoint", Message: "is required"})
	}

	if len(pipeline.Source.ResponseMapping.Fields) == 0 {
		errors = append(errors, ValidationError{Field: "response_mapping.fields", Message: "at least one field is required"})
	}

	return errors
}

// PaginationValidator validates pagination configuration
type PaginationValidator struct{}

// Validate checks that pagination configuration is valid
func (v *PaginationValidator) Validate(config interface{}) []ValidationError {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return []ValidationError{{Field: "config", Message: "not a Pipeline"}}
	}

	var errors []ValidationError

	// Skip validation if pagination is not configured
	if pipeline.Pagination == nil {
		return errors
	}

	switch pipeline.Pagination.Type {
	case PaginationTypePage:
		if pipeline.Pagination.PageParam == "" {
			errors = append(errors, ValidationError{Field: "pagination.page_param", Message: "is required for page pagination"})
		}
		if pipeline.Pagination.PageSize <= 0 {
			errors = append(errors, ValidationError{Field: "pagination.page_size", Message: "must be positive"})
		}
	case PaginationTypeOffset:
		if pipeline.Pagination.OffsetParam == "" {
			errors = append(errors, ValidationError{Field: "pagination.offset_param", Message: "is required for offset pagination"})
		}
		if pipeline.Pagination.LimitParam == "" {
			errors = append(errors, ValidationError{Field: "pagination.limit_param", Message: "is required for offset pagination"})
		}
	case PaginationTypeCursor:
		if pipeline.Pagination.CursorParam == "" {
			errors = append(errors, ValidationError{Field: "pagination.cursor_param", Message: "is required for cursor pagination"})
		}
		if pipeline.Pagination.CursorPath == "" {
			errors = append(errors, ValidationError{Field: "pagination.cursor_path", Message: "is required for cursor pagination"})
		}
	case PaginationTypeLink:
		if pipeline.Pagination.NextLinkPath == "" {
			errors = append(errors, ValidationError{Field: "pagination.next_link_path", Message: "is required for link pagination"})
		}
	default:
		errors = append(errors, ValidationError{Field: "pagination.type", Message: fmt.Sprintf("unknown pagination type: %s", pipeline.Pagination.Type)})
	}

	return errors
}

// AuthValidator handles authentication validation
type AuthValidator struct{}

// Validate checks that authentication configuration is valid
func (v *AuthValidator) Validate(config interface{}) []ValidationError {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return []ValidationError{{Field: "config", Message: "not a Pipeline"}}
	}

	var errors []ValidationError

	// Skip validation if auth is not configured
	if pipeline.Source.Auth == nil {
		return errors
	}

	switch pipeline.Source.Auth.Type {
	case AuthTypeBasic:
		if pipeline.Source.Auth.Basic == nil {
			errors = append(errors, ValidationError{Field: "auth.basic", Message: "is required for basic auth"})
		} else {
			if pipeline.Source.Auth.Basic.Username == "" {
				errors = append(errors, ValidationError{Field: "auth.basic.username", Message: "is required for basic auth"})
			}
			if pipeline.Source.Auth.Basic.Password == "" {
				errors = append(errors, ValidationError{Field: "auth.basic.password", Message: "is required for basic auth"})
			}
		}
	case AuthTypeAPIKey:
		if pipeline.Source.Auth.APIKey == nil {
			errors = append(errors, ValidationError{Field: "auth.api_key", Message: "is required for api_key auth"})
		} else {
			if pipeline.Source.Auth.APIKey.Value == "" {
				errors = append(errors, ValidationError{Field: "auth.api_key.value", Message: "is required for api_key auth"})
			}
			if pipeline.Source.Auth.APIKey.Header == "" && pipeline.Source.Auth.APIKey.QueryParam == "" {
				errors = append(errors, ValidationError{Field: "auth.api_key", Message: "either header or query_param must be specified for api_key auth"})
			}
		}
	case AuthTypeOAuth2:
		if pipeline.Source.Auth.OAuth2 == nil {
			errors = append(errors, ValidationError{Field: "auth.oauth2", Message: "is required for oauth2 auth"})
		} else {
			if pipeline.Source.Auth.OAuth2.TokenURL == "" {
				errors = append(errors, ValidationError{Field: "auth.oauth2.token_url", Message: "is required for oauth2 auth"})
			}
			if pipeline.Source.Auth.OAuth2.ClientID == "" {
				errors = append(errors, ValidationError{Field: "auth.oauth2.client_id", Message: "is required for oauth2 auth"})
			}
			if pipeline.Source.Auth.OAuth2.ClientSecret == "" {
				errors = append(errors, ValidationError{Field: "auth.oauth2.client_secret", Message: "is required for oauth2 auth"})
			}
		}
	default:
		errors = append(errors, ValidationError{Field: "auth.type", Message: fmt.Sprintf("unknown auth type: %s", pipeline.Source.Auth.Type)})
	}

	return errors
}

// SchemaFieldMappingValidator validates that all schema map to response fields
type SchemaFieldMappingValidator struct{}

// Validate checks that all schema source fields exist in response fields
func (v *SchemaFieldMappingValidator) Validate(config interface{}) []ValidationError {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return []ValidationError{{Field: "config", Message: "not a Pipeline"}}
	}

	var errors []ValidationError

	// Create a map of response field names for easy lookup
	fields := make(map[string]struct{})
	for _, field := range pipeline.Source.ResponseMapping.Fields {
		fields[field.Name] = struct{}{}
	}

	// Check that all schema fields have corresponding response fields
	for _, schema := range pipeline.Destination.Schema {
		if _, ok := fields[schema.Source]; !ok {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("schema.%s.source", schema.Name),
				Message: fmt.Sprintf("references undefined field: %s", schema.Source),
			})
		}
	}

	return errors
}
