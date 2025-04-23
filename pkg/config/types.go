package config

// Pipeline represents the full config for one pipeline
type Pipeline struct {
	Name        string      `yaml:"name"`                  // Required: Unique identifier
	Description string      `yaml:"description,omitempty"` // Optional description
	Version     string      `yaml:"version,omitempty"`     // Optional version
	Source      Source      `yaml:"source"`                // Required source configuration
	Pagination  *Pagination `yaml:"pagination,omitempty"`  // Optional pagination config
	Destination Destination `yaml:"destination"`           // Required destination configuration
}

// Source represents API config
type Source struct {
	Type            SourceType        `yaml:"type"`              // Required source type (currently 'rest')
	Endpoint        string            `yaml:"endpoint"`          // Required API URL
	Method          string            `yaml:"method,omitempty"`  // HTTP method (default GET)
	Headers         map[string]string `yaml:"headers,omitempty"` // HTTP headers
	Auth            *Auth             `yaml:"auth,omitempty"`    // Optional authentication
	ResponseMapping ResponseMapping   `yaml:"response_mapping"`  // Required response mapping
}

// SourceType defines currently supported api types
type SourceType string

const (
	SourceTypeREST SourceType = "rest"
)

// Auth defines auth methods.
type Auth struct {
	Type   AuthType    `yaml:"type"`              // Required authentication type
	Basic  *BasicAuth  `yaml:"basic,omitempty"`   // Basic authentication
	APIKey *APIKeyAuth `yaml:"api_key,omitempty"` // API key authentication
	OAuth2 *OAuth2Auth `yaml:"oauth2,omitempty"`  // OAuth2 authentication
}

// AuthType defines current supported authentication types
type AuthType string

const (
	AuthTypeBasic  AuthType = "basic"
	AuthTypeAPIKey AuthType = "api_key"
	AuthTypeOAuth2 AuthType = "oauth2"
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
	TokenURL     string `yaml:"token_url"`
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	Scope        string `yaml:"scope"`
}

// ResponseMapping defines logic for extracting for API responses
type ResponseMapping struct {
	RootPath string  `yaml:"root_path,omitempty"` // (Optional) JSONPath to items array
	Fields   []Field `yaml:"fields"`              // Fields to extract
}

// Field defines a specific field to extract
type Field struct {
	Name string `yaml:"name"` // Name of extracted field
	Path string `yaml:"path"` // JSONPath to field value
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

	// I think these are the most common ones
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
	Name       string `yaml:"name"`                  // Column name
	Type       string `yaml:"type"`                  // Data type (string, integer, etc.)
	Source     string `yaml:"source"`                // Source field name
	PrimaryKey bool   `yaml:"primary_key,omitempty"` // Optional primary key
	Index      bool   `yaml:"index,omitempty"`       // Optional index flag
}
