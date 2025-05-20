package auth

import (
	"Nexus/pkg/config"
	"Nexus/pkg/errors"
)

// CreateHandlerWithRegistry creates an auth handler using the provided registry
func CreateHandlerWithRegistry(registry *AuthRegistry, authConfig *config.Auth) (Handler, error) {
	handler, err := registry.Create(authConfig)
	if err != nil {
		return nil, errors.WrapError(
			err,
			errors.ErrConfiguration,
			"failed to create auth handler",
		)
	}
	return handler, nil
}

// For backward compatibility
var defaultRegistry = NewAuthRegistry()

// CreateHandler creates an auth handler based on configuration using the default registry
func CreateHandler(authConfig *config.Auth) (Handler, error) {
	return CreateHandlerWithRegistry(defaultRegistry, authConfig)
}

// RegisterAuthHandler allows registering custom auth handlers to the default registry
func RegisterAuthHandler(authType config.AuthType, creator AuthCreator) {
	defaultRegistry.Register(authType, creator)
}
