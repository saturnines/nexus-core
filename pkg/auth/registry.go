package auth

import (
	"fmt"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors" // Add this import
	"sync"
)

// AuthCreator defines a function that creates an auth handler from config
type AuthCreator func(*config.Auth) (Handler, error)

// AuthRegistry maintains a registry of auth handler creators
type AuthRegistry struct {
	creators map[config.AuthType]AuthCreator
	mutex    sync.RWMutex
}

// NewAuthRegistry creates a new auth registry with default handlers
func NewAuthRegistry() *AuthRegistry {
	registry := &AuthRegistry{
		creators: make(map[config.AuthType]AuthCreator),
	}

	// Register default handlers
	registry.Register(config.AuthTypeBasic, createBasicAuth)
	registry.Register(config.AuthTypeAPIKey, createAPIKeyAuth)
	registry.Register(config.AuthTypeBearer, createBearerAuth)
	registry.Register(config.AuthTypeOAuth2, createOAuth2Auth)
	return registry
}

// Register adds a new auth creator to the registry
func (r *AuthRegistry) Register(authType config.AuthType, creator AuthCreator) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.creators[authType] = creator
}

// Create creates an auth handler based on the config
func (r *AuthRegistry) Create(authConfig *config.Auth) (Handler, error) {
	r.mutex.RLock()
	creator, exists := r.creators[authConfig.Type]
	r.mutex.RUnlock()

	if !exists {
		return nil, errors.WrapError(
			fmt.Errorf("unsupported auth type: %s", authConfig.Type),
			errors.ErrConfiguration,
			"invalid auth type",
		)
	}

	return creator(authConfig)
}
