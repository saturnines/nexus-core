package auth

import (
	"fmt"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors"
)

// Creator functions for auth handlers

func createBasicAuth(authConfig *config.Auth) (Handler, error) {
	if authConfig.Basic == nil {
		return nil, errors.WrapError(
			fmt.Errorf("basic auth configuration is required"),
			errors.ErrConfiguration,
			"create basic auth",
		)
	}
	return NewBasicAuth(authConfig.Basic.Username, authConfig.Basic.Password), nil
}

func createAPIKeyAuth(authConfig *config.Auth) (Handler, error) {
	if authConfig.APIKey == nil {
		return nil, errors.WrapError(
			fmt.Errorf("api key configuration is required"),
			errors.ErrConfiguration,
			"create API key auth",
		)
	}
	return NewAPIKeyAuth(
		authConfig.APIKey.Header,
		authConfig.APIKey.QueryParam,
		authConfig.APIKey.Value,
	), nil
}

func createBearerAuth(authConfig *config.Auth) (Handler, error) {
	if authConfig.Bearer == nil {
		return nil, errors.WrapError(
			fmt.Errorf("bearer token configuration is required"),
			errors.ErrConfiguration,
			"create bearer auth",
		)
	}
	return NewBearerAuth(authConfig.Bearer.Token), nil
}

func createOAuth2Auth(authConfig *config.Auth) (Handler, error) {
	if authConfig.OAuth2 == nil {
		return nil, errors.WrapError(
			fmt.Errorf("oauth2 configuration is required"),
			errors.ErrConfiguration,
			"create OAuth2 auth",
		)
	}

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
}
