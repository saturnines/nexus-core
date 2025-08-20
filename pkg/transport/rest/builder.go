// pkg/transport/rest/builder.go
package rest

import (
	"context"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/saturnines/nexus-core/pkg/auth"
)

// Builder builds REST HTTP requests.
type Builder struct {
	URL         string
	Method      string
	Headers     map[string]string
	QueryParams map[string]string
	AuthHandler auth.Handler
}

// NewBuilder constructs a Builder.
// Method defaults to GET if empty.
func NewBuilder(
	url, method string,
	headers, params map[string]string,
	authHandler auth.Handler,
) *Builder {
	if method == "" {
		method = http.MethodGet
	}
	return &Builder{
		URL:         url,
		Method:      method,
		Headers:     headers,
		QueryParams: params,
		AuthHandler: authHandler,
	}
}

// Build creates an HTTP request.
func (b *Builder) Build(ctx context.Context) (*http.Request, error) {
	// Substitute template variables in the URL
	url := b.substituteTemplateVariables(b.URL)
	
	req, err := http.NewRequestWithContext(ctx, b.Method, url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range b.Headers {
		// Also substitute template variables in header values
		req.Header.Set(k, b.substituteTemplateVariables(v))
	}

	if len(b.QueryParams) > 0 {
		q := req.URL.Query()
		for k, v := range b.QueryParams {
			// Also substitute template variables in query param values
			q.Set(k, b.substituteTemplateVariables(v))
		}
		req.URL.RawQuery = q.Encode()
	}

	if b.AuthHandler != nil {
		if err := b.AuthHandler.ApplyAuth(req); err != nil {
			return nil, err
		}
	}

	return req, nil
}

// substituteTemplateVariables replaces {{VAR_NAME}} with environment variable values
func (b *Builder) substituteTemplateVariables(text string) string {
	// This regex matches {{VARIABLE_NAME}} patterns
	templatePattern := regexp.MustCompile(`\{\{([^}]+)\}\}`)
	
	return templatePattern.ReplaceAllStringFunc(text, func(match string) string {
		// Extract variable name (remove {{ and }})
		varName := strings.TrimSpace(match[2 : len(match)-2])
		
		// Get environment variable value
		if value := os.Getenv(varName); value != "" {
			return value
		}
		return match
	})
}
