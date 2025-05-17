package auth

import (
	"Nexus/pkg/config"
	"fmt"
)

// CreateHandler creates an auth handler based on configuration
func CreateHandler(authConfig *config.Auth) (Handler, error) {
	switch authConfig.Type {
	case config.AuthTypeBasic:
		return NewBasicAuth(authConfig.Basic.Username, authConfig.Basic.Password), nil

	case config.AuthTypeAPIKey:
		return NewAPIKeyAuth(authConfig.APIKey.Header, authConfig.APIKey.QueryParam, authConfig.APIKey.Value), nil

	case config.AuthTypeBearer:
		return NewBearerAuth(authConfig.Bearer.Token), nil

	case config.AuthTypeOAuth2:
		refreshBefore := 60 // Default
		if authConfig.OAuth2.RefreshBefore > 0 {
			refreshBefore = authConfig.OAuth2.RefreshBefore
		}

		return NewOAuth2Auth(
			authConfig.OAuth2.TokenURL,
			authConfig.OAuth2.ClientID,
			authConfig.OAuth2.ClientSecret,
			authConfig.OAuth2.Scope,
			authConfig.OAuth2.ExtraParams,
			refreshBefore,
		)

	default:
		return nil, fmt.Errorf("unsupported auth type: %s", authConfig.Type)
	}
}
