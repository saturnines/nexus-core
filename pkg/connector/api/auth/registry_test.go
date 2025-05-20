package auth

import (
	"Nexus/pkg/config"
	"Nexus/pkg/errors"
	"testing"
)

func TestAuthRegistry(t *testing.T) {
	// Create a new registry
	registry := NewAuthRegistry()

	// Test creating existing handler (Basic Auth)
	basicConfig := &config.Auth{
		Type: config.AuthTypeBasic,
		Basic: &config.BasicAuth{
			Username: "testuser",
			Password: "testpass",
		},
	}

	handler, err := registry.Create(basicConfig)
	if err != nil {
		t.Fatalf("Failed to create basic auth handler: %v", err)
	}

	basicHandler, ok := handler.(*BasicAuth)
	if !ok {
		t.Fatal("Handler is not a BasicAuth")
	}

	if basicHandler.Username != "testuser" || basicHandler.Password != "testpass" {
		t.Error("BasicAuth handler has incorrect values")
	}

	// Test registering and using a custom handler
	customType := config.AuthType("custom")

	registry.Register(customType, func(authConfig *config.Auth) (Handler, error) {
		return NewBasicAuth("custom", "custom"), nil
	})

	customConfig := &config.Auth{
		Type: customType,
	}

	handler, err = registry.Create(customConfig)
	if err != nil {
		t.Fatalf("Failed to create custom auth handler: %v", err)
	}

	customHandler, ok := handler.(*BasicAuth)
	if !ok {
		t.Fatal("Custom handler is not a BasicAuth")
	}

	if customHandler.Username != "custom" || customHandler.Password != "custom" {
		t.Error("Custom handler has incorrect values")
	}

	// Test unsupported type
	unsupportedConfig := &config.Auth{
		Type: config.AuthType("unsupported"),
	}

	_, err = registry.Create(unsupportedConfig)
	if err == nil {
		t.Fatal("Expected error for unsupported auth type")
	}

	// Verify error is wrapped correctly
	if !errors.Is(err, errors.ErrConfiguration) {
		t.Errorf("Expected ErrConfiguration, got: %v", err)
	}
}
