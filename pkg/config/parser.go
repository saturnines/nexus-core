// pkg/config/parser.go

package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ConfigLoader defines the interface for loading configs
type ConfigLoader interface {
	Load(path string) (interface{}, error)
	Parse(data []byte) (interface{}, error)
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field      string      // Field name with error
	Message    string      // Error message
	ConfigPath string      // Path in configuration (for nested fields)
	Value      interface{} // The problematic value (if applicable)
}

// Returns the string representation of validation error
func (e ValidationError) Error() string {
	if e.ConfigPath != "" && e.Value != nil {
		return fmt.Sprintf("%s: %s (at %s, value: %v)", e.Field, e.Message, e.ConfigPath, e.Value)
	}
	if e.ConfigPath != "" {
		return fmt.Sprintf("%s: %s (at %s)", e.Field, e.Message, e.ConfigPath)
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// Validator interface for configuration validation
type Validator interface {
	Validate(config interface{}) []ValidationError
}

// DefaultValueSetter handles the interface for setting default values
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

// ReferenceResolver resolves configuration references
type ReferenceResolver struct{}

// ResolveReferences replaces reference placeholders with actual configuration
func (r *ReferenceResolver) ResolveReferences(pipeline *Pipeline) error {
	// Skip if no references defined
	if len(pipeline.References) == 0 {
		return nil
	}

	// Resolve auth reference if specified
	if pipeline.Source.AuthRef != "" {
		authRef, ok := pipeline.References[pipeline.Source.AuthRef]
		if !ok {
			return fmt.Errorf("undefined auth reference: %s", pipeline.Source.AuthRef)
		}

		// Convert to Auth type
		authConfig, ok := authRef.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid auth reference format: %v", authRef)
		}

		// Marshal and unmarshal to convert to Auth struct
		authBytes, err := yaml.Marshal(authConfig)
		if err != nil {
			return fmt.Errorf("failed to process auth reference: %w", err)
		}

		var auth Auth
		if err := yaml.Unmarshal(authBytes, &auth); err != nil {
			return fmt.Errorf("failed to process auth reference: %w", err)
		}

		pipeline.Source.Auth = &auth
	}

	// Resolve pagination reference if specified
	if pipeline.PaginationRef != "" {
		paginationRef, ok := pipeline.References[pipeline.PaginationRef]
		if !ok {
			return fmt.Errorf("undefined pagination reference: %s", pipeline.PaginationRef)
		}

		// Convert to Pagination type
		paginationConfig, ok := paginationRef.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid pagination reference format: %v", paginationRef)
		}

		// Marshal and unmarshal to convert to Pagination struct
		paginationBytes, err := yaml.Marshal(paginationConfig)
		if err != nil {
			return fmt.Errorf("failed to process pagination reference: %w", err)
		}

		var pagination Pagination
		if err := yaml.Unmarshal(paginationBytes, &pagination); err != nil {
			return fmt.Errorf("failed to process pagination reference: %w", err)
		}

		pipeline.Pagination = &pagination
	}

	return nil
}

// PipelineLoader uses ConfigLoader for Pipeline configurations
type PipelineLoader struct {
	expander          VariableExpander
	validators        []Validator
	defaultSetter     DefaultValueSetter
	referenceResolver *ReferenceResolver
}

// NewPipelineLoader creates a new PipelineLoader with the given components
func NewPipelineLoader(
	expander VariableExpander,
	defaultSetter DefaultValueSetter,
	validators ...Validator,
) *PipelineLoader {
	return &PipelineLoader{
		expander:          expander,
		validators:        validators,
		defaultSetter:     defaultSetter,
		referenceResolver: &ReferenceResolver{},
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

// LoadWithEnvironment loads a config file with environment-specific overrides
func (l *PipelineLoader) LoadWithEnvironment(path string, environment string) (interface{}, error) {
	// Load base configuration
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Check for environment-specific configuration
	if environment != "" {
		baseDir := filepath.Dir(path)
		baseFileName := filepath.Base(path)
		ext := filepath.Ext(baseFileName)
		baseName := strings.TrimSuffix(baseFileName, ext)
		envPath := filepath.Join(baseDir, fmt.Sprintf("%s.%s%s", baseName, environment, ext))

		if _, err := os.Stat(envPath); err == nil {
			envData, err := os.ReadFile(envPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read environment config: %w", err)
			}

			// Merge base with environment-specific configuration
			mergedData, err := mergeYAML(data, envData)
			if err != nil {
				return nil, fmt.Errorf("failed to merge environment config: %w", err)
			}
			data = mergedData
		}
	}

	return l.Parse(data)
}

// mergeYAML merges two YAML byte slices
func mergeYAML(base, overlay []byte) ([]byte, error) {
	// Unmarshal base config
	var baseMap map[string]interface{}
	if err := yaml.Unmarshal(base, &baseMap); err != nil {
		return nil, fmt.Errorf("failed to parse base config: %w", err)
	}

	// Unmarshal overlay config
	var overlayMap map[string]interface{}
	if err := yaml.Unmarshal(overlay, &overlayMap); err != nil {
		return nil, fmt.Errorf("failed to parse overlay config: %w", err)
	}

	// Merge recursively
	merged := mergeMap(baseMap, overlayMap)

	// Marshal back to YAML
	result, err := yaml.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("failed to encode merged config: %w", err)
	}

	return result, nil
}

// mergeMap recursively merges two maps
func mergeMap(base, overlay map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy base values
	for k, v := range base {
		result[k] = v
	}

	// Apply overlay values, with special handling for maps
	for k, overlayVal := range overlay {
		baseVal, exists := base[k]

		// If both values are maps, merge them recursively
		if baseMap, baseIsMap := baseVal.(map[string]interface{}); exists && baseIsMap {
			if overlayMap, overlayIsMap := overlayVal.(map[string]interface{}); overlayIsMap {
				result[k] = mergeMap(baseMap, overlayMap)
				continue
			}
		}

		// For all other cases, overlay value overwrites base value
		result[k] = overlayVal
	}

	return result
}

// Parse parses a yaml config
func (l *PipelineLoader) Parse(data []byte) (interface{}, error) {
	if l.expander != nil {
		// Use smart expansion that only expands ${VAR} patterns, not $var patterns
		data = l.smartExpand(data)
	}

	var pipeline Pipeline
	if err := yaml.Unmarshal(data, &pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// add GraphQL branch
	switch pipeline.Source.Type {
	case SourceTypeGraphQL:
		if pipeline.Source.GraphQLConfig == nil {
			return nil, fmt.Errorf("graphql config missing for source.type=graphql")
		}
		if pipeline.Source.GraphQLConfig.Endpoint == "" {
			return nil, fmt.Errorf("graphql.endpoint is required")
		}
		if pipeline.Source.GraphQLConfig.Query == "" {
			return nil, fmt.Errorf("graphql.query is required")
		}
		if pipeline.Source.GraphQLConfig.Headers == nil {
			pipeline.Source.GraphQLConfig.Headers = map[string]string{
				"Content-Type": "application/json",
			}
		}
		// propagate Auth from GraphQL block
		if pipeline.Source.GraphQLConfig.Auth != nil {
			pipeline.Source.Auth = pipeline.Source.GraphQLConfig.Auth
		}
		pipeline.Source.ResponseMapping = pipeline.Source.GraphQLConfig.ResponseMapping
		// use GraphQL pagination if provided
		if pipeline.Source.GraphQLConfig.Pagination != nil {
			pipeline.Pagination = pipeline.Source.GraphQLConfig.Pagination
		}

	case SourceTypeREST:
		// leave REST checks to existing validators
	default:
		return nil, fmt.Errorf("unsupported source.type: %s", pipeline.Source.Type)
	}

	if err := l.referenceResolver.ResolveReferences(&pipeline); err != nil {
		return nil, err
	}

	if l.defaultSetter != nil {
		l.defaultSetter.SetDefaults(&pipeline)
	}

	var allErrors []ValidationError
	for _, validator := range l.validators {
		errs := validator.Validate(&pipeline)
		allErrors = append(allErrors, errs...)
	}
	if len(allErrors) > 0 {
		return nil, fmt.Errorf("validation errors: %v", allErrors)
	}

	return &pipeline, nil
}

func (l *PipelineLoader) smartExpand(data []byte) []byte {
	// This regex matches ${...} but not $variable
	// It captures the variable name inside the braces
	envVarPattern := regexp.MustCompile(`\$\{([^}]+)\}`)

	result := envVarPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		varName := string(match[2 : len(match)-1])
		value := os.Getenv(varName)
		return []byte(value)
	})

	return result
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
		if pipeline.Source.Type == SourceTypeGraphQL {
			pipeline.Source.Method = "POST"
		} else {
			pipeline.Source.Method = "GET"
		}
	}

	// Set defaults for authentication
	if pipeline.Source.Auth != nil {
		switch pipeline.Source.Auth.Type {
		case AuthTypeOAuth2:
			if pipeline.Source.Auth.OAuth2 != nil {
				// Default token type for OAuth2
				if pipeline.Source.Auth.OAuth2.TokenType == "" {
					pipeline.Source.Auth.OAuth2.TokenType = "Bearer"
				}
			}
		}
	}

	// Set defaults for pagination
	if pipeline.Pagination != nil {
		switch pipeline.Pagination.Type {
		case PaginationTypePage:
			// Default page size
			if pipeline.Pagination.PageSize <= 0 {
				pipeline.Pagination.PageSize = 20
			}
			// Default page parameter name
			if pipeline.Pagination.PageParam == "" && pipeline.Pagination.SizeParam != "" {
				pipeline.Pagination.PageParam = "page"
			}
		case PaginationTypeOffset:
			// Default offset increment (limit)
			if pipeline.Pagination.OffsetIncrement <= 0 {
				pipeline.Pagination.OffsetIncrement = 20
			}
		}
	}

	// Set defaults for retry configuration
	if pipeline.RetryConfig != nil {
		if pipeline.RetryConfig.MaxAttempts <= 0 {
			pipeline.RetryConfig.MaxAttempts = 3
		}
		if pipeline.RetryConfig.BackoffMultiplier <= 0 {
			pipeline.RetryConfig.BackoffMultiplier = 2.0
		}
		if pipeline.RetryConfig.InitialBackoff <= 0 {
			pipeline.RetryConfig.InitialBackoff = 1 // 1 second
		}
	}
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

	if pipeline.Source.Type == SourceTypeREST && pipeline.Source.Endpoint == "" {
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

	// Common validation for all pagination types
	if pipeline.Pagination.Type == "" {
		errors = append(errors, ValidationError{
			Field:   "pagination.type",
			Message: "pagination type is required",
		})
		return errors
	}

	switch pipeline.Pagination.Type {
	case PaginationTypePage:
		if pipeline.Pagination.PageParam == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.page_param",
				Message: "is required for page pagination",
			})
		}
		if pipeline.Pagination.PageSize <= 0 {
			errors = append(errors, ValidationError{
				Field:   "pagination.page_size",
				Message: "must be positive",
			})
		}
	case PaginationTypeOffset:
		if pipeline.Pagination.OffsetParam == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.offset_param",
				Message: "is required for offset pagination",
			})
		}
		if pipeline.Pagination.LimitParam == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.limit_param",
				Message: "is required for offset pagination",
			})
		}
	case PaginationTypeCursor:
		if pipeline.Pagination.CursorParam == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.cursor_param",
				Message: "is required for cursor pagination",
			})
		}
		// For cursor pagination, we need either cursor_path or has_more_path
		if pipeline.Pagination.CursorPath == "" && pipeline.Pagination.HasMorePath == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.cursor_path",
				Message: "either cursor_path or has_more_path is required for cursor pagination",
			})
		}
	case PaginationTypeLink:
		if pipeline.Pagination.NextLinkPath == "" {
			errors = append(errors, ValidationError{
				Field:   "pagination.next_link_path",
				Message: "is required for link pagination",
			})
		}
	default:
		errors = append(errors, ValidationError{
			Field:   "pagination.type",
			Message: fmt.Sprintf("unknown pagination type: %s", pipeline.Pagination.Type),
			Value:   string(pipeline.Pagination.Type),
		})
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

	// Check if we're using auth reference
	if pipeline.Source.AuthRef != "" {
		if _, ok := pipeline.References[pipeline.Source.AuthRef]; !ok {
			errors = append(errors, ValidationError{
				Field:   "source.auth_ref",
				Message: fmt.Sprintf("references undefined auth configuration: %s", pipeline.Source.AuthRef),
				Value:   pipeline.Source.AuthRef,
			})
		}
		return errors
	}

	// Validate based on auth type
	switch pipeline.Source.Auth.Type {
	case AuthTypeBasic:
		if pipeline.Source.Auth.Basic == nil {
			errors = append(errors, ValidationError{
				Field:   "auth.basic",
				Message: "is required for basic auth",
			})
		} else {
			if pipeline.Source.Auth.Basic.Username == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.basic.username",
					Message: "is required for basic auth",
				})
			}
			if pipeline.Source.Auth.Basic.Password == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.basic.password",
					Message: "is required for basic auth",
				})
			}
		}
	case AuthTypeAPIKey:
		if pipeline.Source.Auth.APIKey == nil {
			errors = append(errors, ValidationError{
				Field:   "auth.api_key",
				Message: "is required for api_key auth",
			})
		} else {
			if pipeline.Source.Auth.APIKey.Value == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.api_key.value",
					Message: "is required for api_key auth",
				})
			}
			if pipeline.Source.Auth.APIKey.Header == "" && pipeline.Source.Auth.APIKey.QueryParam == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.api_key",
					Message: "either header or query_param must be specified for api_key auth",
				})
			}
		}
	case AuthTypeOAuth2:
		if pipeline.Source.Auth.OAuth2 == nil {
			errors = append(errors, ValidationError{
				Field:   "auth.oauth2",
				Message: "is required for oauth2 auth",
			})
		} else {
			if pipeline.Source.Auth.OAuth2.TokenURL == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.oauth2.token_url",
					Message: "is required for oauth2 auth",
				})
			}
			if pipeline.Source.Auth.OAuth2.ClientID == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.oauth2.client_id",
					Message: "is required for oauth2 auth",
				})
			}
			if pipeline.Source.Auth.OAuth2.ClientSecret == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.oauth2.client_secret",
					Message: "is required for oauth2 auth",
				})
			}
		}
	case AuthTypeBearer:
		if pipeline.Source.Auth.Bearer == nil {
			errors = append(errors, ValidationError{
				Field:   "auth.bearer",
				Message: "is required for bearer auth",
			})
		} else {
			if pipeline.Source.Auth.Bearer.Token == "" {
				errors = append(errors, ValidationError{
					Field:   "auth.bearer.token",
					Message: "is required for bearer auth",
				})
			}
		}
	default:
		errors = append(errors, ValidationError{
			Field:   "auth.type",
			Message: fmt.Sprintf("unknown auth type: %s", pipeline.Source.Auth.Type),
			Value:   string(pipeline.Source.Auth.Type),
		})
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
				Value:   schema.Source,
			})
		}
	}

	return errors
}

// RetryConfigValidator validates retry configuration
type RetryConfigValidator struct{}

// Validate checks that retry configuration is valid
func (v *RetryConfigValidator) Validate(config interface{}) []ValidationError {
	pipeline, ok := config.(*Pipeline)
	if !ok {
		return []ValidationError{{Field: "config", Message: "not a Pipeline"}}
	}

	var errors []ValidationError

	// Skip validation if retry config is not present
	if pipeline.RetryConfig == nil {
		return errors
	}

	// Validate retry configuration
	if pipeline.RetryConfig.MaxAttempts <= 0 {
		errors = append(errors, ValidationError{
			Field:   "retry_config.max_attempts",
			Message: "must be positive",
			Value:   pipeline.RetryConfig.MaxAttempts,
		})
	}

	if pipeline.RetryConfig.BackoffMultiplier <= 0 {
		errors = append(errors, ValidationError{
			Field:   "retry_config.backoff_multiplier",
			Message: "must be positive",
			Value:   pipeline.RetryConfig.BackoffMultiplier,
		})
	}

	if pipeline.RetryConfig.InitialBackoff < 0 {
		errors = append(errors, ValidationError{
			Field:   "retry_config.initial_backoff",
			Message: "cannot be negative",
			Value:   pipeline.RetryConfig.InitialBackoff,
		})
	}

	return errors
}
