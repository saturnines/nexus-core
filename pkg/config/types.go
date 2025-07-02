// pkg/config/types.go

package config

// Pipeline represents the full config for one pipeline
type Pipeline struct {
	Name          string                 `yaml:"name"`                     // Required: Unique identifier
	Description   string                 `yaml:"description,omitempty"`    // Optional description
	Version       string                 `yaml:"version,omitempty"`        // Optional version
	Source        Source                 `yaml:"source"`                   // Required source configuration
	Pagination    *Pagination            `yaml:"pagination,omitempty"`     // Optional pagination config
	PaginationRef string                 `yaml:"pagination_ref,omitempty"` // Reference to a pagination config
	Destination   Destination            `yaml:"destination"`              // Required destination configuration
	RetryConfig   *RetryConfig           `yaml:"retry_config,omitempty"`   // Optional retry configuration
	References    map[string]interface{} `yaml:"references,omitempty"`     // Reusable configuration blocks
}

type GraphQLSource struct {
	Endpoint        string                 `yaml:"endpoint"`
	Query           string                 `yaml:"query"`
	Variables       map[string]interface{} `yaml:"variables,omitempty"`
	Headers         map[string]string      `yaml:"headers,omitempty"`
	Auth            *Auth                  `yaml:"auth,omitempty"`
	ResponseMapping ResponseMapping        `yaml:"response_mapping"`
	Pagination      *Pagination            `yaml:"pagination,omitempty"`
}

// RetryConfig represents retry settings for failed requests
type RetryConfig struct {
	MaxAttempts       int     `yaml:"max_attempts"`                 // Maximum number of retry attempts
	InitialBackoff    float64 `yaml:"initial_backoff,omitempty"`    // Initial backoff in seconds
	BackoffMultiplier float64 `yaml:"backoff_multiplier,omitempty"` // Multiplier for exponential backoff
	RetryableStatuses []int   `yaml:"retryable_statuses,omitempty"` // HTTP status codes to retry
}

// Source represents API config
type Source struct {
	Type            SourceType        `yaml:"type"`                   // Required source type (currently 'rest')
	Endpoint        string            `yaml:"endpoint"`               // Required API URL
	Method          string            `yaml:"method,omitempty"`       // HTTP method (default GET)
	Headers         map[string]string `yaml:"headers,omitempty"`      // HTTP headers
	QueryParams     map[string]string `yaml:"query_params,omitempty"` // Query parameters
	Auth            *Auth             `yaml:"auth,omitempty"`         // Direct authentication configuration
	AuthRef         string            `yaml:"auth_ref,omitempty"`     // Reference to an auth config
	ResponseMapping ResponseMapping   `yaml:"response_mapping"`       // Required response mapping

	// GraphQL
	GraphQLConfig *GraphQLSource `yaml:"graphql,omitempty"`
}

// SourceType defines currently supported api types
type SourceType string

const (
	SourceTypeREST    SourceType = "rest"
	SourceTypeGraphQL SourceType = "graphql"
)

// Auth defines auth methods.
type Auth struct {
	Type   AuthType    `yaml:"type"`              // Required authentication type
	Basic  *BasicAuth  `yaml:"basic,omitempty"`   // Basic authentication
	APIKey *APIKeyAuth `yaml:"api_key,omitempty"` // API key authentication
	OAuth2 *OAuth2Auth `yaml:"oauth2,omitempty"`  // OAuth2 authentication
	Bearer *BearerAuth `yaml:"bearer,omitempty"`  // Bearer token authentication
}

// AuthType defines current supported authentication types
type AuthType string

const (
	AuthTypeBasic  AuthType = "basic"
	AuthTypeAPIKey AuthType = "api_key"
	AuthTypeOAuth2 AuthType = "oauth2"
	AuthTypeBearer AuthType = "bearer"
)

// BasicAuth contains auth credentials for the api
type BasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// APIKeyAuth contains API details
type APIKeyAuth struct {
	Header     string `yaml:"header,omitempty"`      // Header name
	QueryParam string `yaml:"query_param,omitempty"` // Query parameter name
	Value      string `yaml:"value"`                 // API key value
}

// OAuth2Auth contains OAuth2 auth details
type OAuth2Auth struct {
	TokenURL      string            `yaml:"token_url"`
	ClientID      string            `yaml:"client_id"`
	ClientSecret  string            `yaml:"client_secret"`
	Scope         string            `yaml:"scope,omitempty"`
	TokenType     string            `yaml:"token_type,omitempty"`     // Token type (default: Bearer)
	ExtraParams   map[string]string `yaml:"extra_params,omitempty"`   // Additional parameters for token request
	RefreshBefore int               `yaml:"refresh_before,omitempty"` // Seconds before expiry to refresh token
}

// BearerAuth represents simple bearer token authentication
type BearerAuth struct {
	Token string `yaml:"token"` // Bearer token value
}

// ResponseMapping defines logic for extracting for API responses
type ResponseMapping struct {
	RootPath        string            `yaml:"root_path,omitempty"`       // JSONPath to items array
	Fields          []Field           `yaml:"fields"`                    // Fields to extract
	Transformations map[string]string `yaml:"transformations,omitempty"` // Field transformations
	ErrorPath       string            `yaml:"error_path,omitempty"`      // Path to error message in response
	SuccessPath     string            `yaml:"success_path,omitempty"`    // Path to success flag in response
}

// Field defines a specific field to extract
type Field struct {
	Name         string          `yaml:"name"`                    // Name of extracted field
	Path         string          `yaml:"path"`                    // JSONPath to field value
	Type         string          `yaml:"type,omitempty"`          // Data type for conversion
	DefaultValue interface{}     `yaml:"default_value,omitempty"` // Default value if field is missing
	Transform    *FieldTransform `yaml:"transform,omitempty"`     // NEW: Transform configuration
}

// Pagination defines different pagination types
type Pagination struct {
	Type PaginationType `yaml:"type"` // Pagination type (page, offset, cursor, link)

	// Page-based pagination (if Type="page")
	PageParam      string `yaml:"page_param,omitempty"`
	SizeParam      string `yaml:"size_param,omitempty"`
	PageSize       int    `yaml:"page_size,omitempty"`
	TotalPagesPath string `yaml:"total_pages_path,omitempty"`

	// Offset-based pagination (if Type="offset")
	OffsetParam     string `yaml:"offset_param,omitempty"`
	LimitParam      string `yaml:"limit_param,omitempty"`
	OffsetIncrement int    `yaml:"offset_increment,omitempty"`
	TotalCountPath  string `yaml:"total_count_path,omitempty"`

	// Cursor-based pagination (if Type="cursor")
	CursorParam string `yaml:"cursor_param,omitempty"`
	CursorPath  string `yaml:"cursor_path,omitempty"`
	HasMorePath string `yaml:"has_more_path,omitempty"`

	// Link-based pagination (if Type="link")
	NextLinkPath string `yaml:"next_link_path,omitempty"`
	LinkHeader   bool   `yaml:"link_header,omitempty"` // Use standard Link header for navigation
}

// PaginationType defines supported pagination types
type PaginationType string

const (
	PaginationTypePage   PaginationType = "page"
	PaginationTypeOffset PaginationType = "offset"
	PaginationTypeCursor PaginationType = "cursor"
	PaginationTypeLink   PaginationType = "link"
)

// Destination defines the data storage destination
type Destination struct {
	Type   DestinationType `yaml:"type"`   // Destination type (postgres, mongodb, etc.)
	Table  string          `yaml:"table"`  // Table/collection name
	Schema []Schema        `yaml:"schema"` // Schema definitions
}

// DestinationType defines supported destination types
type DestinationType string

const (
	DestinationPostgres DestinationType = "postgres"
	DestinationMongoDB  DestinationType = "mongodb"
)

// Schema defines the database schema fields
type Schema struct {
	Name       string   `yaml:"name"`                  // Column name
	Type       string   `yaml:"type"`                  // Data type (string, integer, etc.)
	Source     string   `yaml:"source"`                // Source field name
	PrimaryKey bool     `yaml:"primary_key,omitempty"` // Optional primary key
	Index      bool     `yaml:"index,omitempty"`       // Optional index flag
	Required   bool     `yaml:"required,omitempty"`    // Whether field is required
	Unique     bool     `yaml:"unique,omitempty"`      // Whether field must be unique
	Validators []string `yaml:"validators,omitempty"`  // Validation rules

}

// FieldTransorm defines tranformation to apply for whatever field
// Example below
type FieldTransform struct {
	Type   string                 `yaml:"type"`             // Transform type: "date", "int", "string", etc.
	Config map[string]interface{} `yaml:"config,omitempty"` // Transform specific config
	Chain  []FieldTransform       `yaml:"chain,omitempty"`  // For chaining multiple transforms
}
