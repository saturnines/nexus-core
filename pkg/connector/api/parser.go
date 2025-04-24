package api

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"reflect"
)

// ConfigLoader defines the interface for laoding configs

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

// PipelineLoader uses  ConfigLoader for Pipeline configurations
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

// Loads a new pipeline config from YAML file
func (l *PipelineLoader) Load(path string) (interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return l.Parse(data)
}
