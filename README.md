# Nexus Core

[![Go Version](https://img.shields.io/github/go-mod/go-version/saturnines/nexus-core)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/saturnines/nexus-core)](https://goreportcard.com/report/github.com/saturnines/nexus-core)
[![GitHub release](https://img.shields.io/github/release/saturnines/nexus-core.svg)](https://github.com/saturnines/nexus-core/releases)

A universal API data extractor built in Go. Configure any REST/GraphQL API with YAML and extract all data automatically with built-in pagination, authentication, and error handling.

## ✨ Features

- **🔌 Universal API Support** - Works with any REST or GraphQL API
- **🔐 Authentication** - Basic, Bearer, API Key, OAuth2 with automatic token refresh
- **📄 Smart Pagination** - Cursor, offset, page-based, and link header pagination
- **🎯 Advanced Field Extraction** - JSONPath with nested objects, arrays, and wildcards
- **🔄 Automatic Retries** - Exponential backoff with configurable retry policies
- **⚡ Rate Limit Handling** - Detects 429 responses (more APIs coming soon)
- **🔒 OAuth2 Token Management** - Automatic token refresh and caching
- **🛡️ Reliable** - Comprehensive error handling and logging

## 🚀 Quick Start

### Installation

```bash
go get github.com/saturnines/nexus-core@latest
```

### Simple Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/saturnines/nexus-core/pkg/config"
    "github.com/saturnines/nexus-core/pkg/core"
)

func main() {
    cfg := &config.Pipeline{
        Name: "jsonplaceholder-posts",
        Source: config.Source{
            Type:     config.SourceTypeREST,
            Endpoint: "https://jsonplaceholder.typicode.com/posts",
            ResponseMapping: config.ResponseMapping{
                Fields: []config.Field{
                    {Name: "id", Path: "id"},
                    {Name: "title", Path: "title"},
                    {Name: "author_id", Path: "userId"},
                },
            },
        },
    }

    connector, err := core.NewConnector(cfg)
    if err != nil {
        log.Fatal(err)
    }

    results, err := connector.Extract(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Extracted %d records\n", len(results))
    for _, result := range results[:3] { // Show first 3
        fmt.Printf("Post %v: %v\n", result["id"], result["title"])
    }
}
```

## 📖 Configuration Examples

### REST API with Authentication

```yaml
name: stripe-customers
source:
  type: rest
  endpoint: https://api.stripe.com/v1/customers
  auth:
    type: bearer
    bearer:
      token: ${STRIPE_SECRET_KEY}
  response_mapping:
    root_path: data
    fields:
      - name: id
        path: id
      - name: email
        path: email
      - name: created
        path: created
pagination:
  type: cursor
  cursor_param: starting_after
  cursor_path: data.-1.id
  has_more_path: has_more
```

### GraphQL API with Variables

```yaml
name: github-repositories
source:
  type: graphql
  graphql:
    endpoint: https://api.github.com/graphql
    query: |
      query($first: Int!, $after: String) {
        viewer {
          repositories(first: $first, after: $after) {
            edges {
              node {
                id
                name
                description
                stargazerCount
              }
              cursor
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    variables:
      first: 10
    headers:
      Authorization: "Bearer ${GITHUB_TOKEN}"
    response_mapping:
      root_path: viewer.repositories.edges
      fields:
        - name: id
          path: node.id
        - name: name
          path: node.name
        - name: stars
          path: node.stargazerCount
  pagination:
    type: cursor
    cursor_param: after
    cursor_path: data.viewer.repositories.pageInfo.endCursor
    has_more_path: data.viewer.repositories.pageInfo.hasNextPage
```

### OAuth2 with Automatic Token Refresh

```yaml
name: google-analytics
source:
  type: rest
  endpoint: https://analyticsreporting.googleapis.com/v4/reports:batchGet
  auth:
    type: oauth2
    oauth2:
      token_url: https://oauth2.googleapis.com/token
      client_id: ${GOOGLE_CLIENT_ID}
      client_secret: ${GOOGLE_CLIENT_SECRET}
      scope: "https://www.googleapis.com/auth/analytics.readonly"
      refresh_before: 300  # Refresh 5 minutes before expiry
  response_mapping:
    root_path: reports.0.data.rows
    fields:
      - name: page_path
        path: dimensions.0
      - name: page_views
        path: metrics.0.values.0
```

### Advanced Field Extraction

```yaml
name: complex-api
source:
  type: rest
  endpoint: https://api.example.com/users
  response_mapping:
    root_path: data.users
    fields:
      # Simple fields
      - name: id
        path: id
      - name: email
        path: email
      
      # Nested objects
      - name: street
        path: address.street
      - name: city
        path: address.city
      
      # Array indexing
      - name: first_phone
        path: phones[0].number
      - name: primary_skill
        path: skills[0]
      
      # Array wildcards
      - name: all_tags
        path: tags[*]
      
      # Default values
      - name: status
        path: status
        default_value: "active"
      
      # Deep nesting
      - name: manager_email
        path: department.manager.contact.email
```

## 🔧 Authentication Methods

### Basic Authentication
```yaml
auth:
  type: basic
  basic:
    username: ${API_USERNAME}
    password: ${API_PASSWORD}
```

### API Key (Header)
```yaml
auth:
  type: api_key
  api_key:
    header: X-API-Key
    value: ${API_KEY}
```

### API Key (Query Parameter)
```yaml
auth:
  type: api_key
  api_key:
    query_param: api_key
    value: ${API_KEY}
```

### Bearer Token
```yaml
auth:
  type: bearer
  bearer:
    token: ${ACCESS_TOKEN}
```

### OAuth2 Client Credentials
```yaml
auth:
  type: oauth2
  oauth2:
    token_url: https://api.example.com/oauth/token
    client_id: ${CLIENT_ID}
    client_secret: ${CLIENT_SECRET}
    scope: "read:data"
    extra_params:
      audience: "https://api.example.com"
```

## 📄 Pagination Support

### Cursor-Based Pagination
```yaml
pagination:
  type: cursor
  cursor_param: cursor
  cursor_path: pagination.next_cursor
  has_more_path: pagination.has_more
```

### Offset-Based Pagination
```yaml
pagination:
  type: offset
  offset_param: offset
  limit_param: limit
  offset_increment: 100
  total_count_path: meta.total_count
```

### Page-Based Pagination
```yaml
pagination:
  type: page
  page_param: page
  size_param: per_page
  page_size: 50
  total_pages_path: meta.total_pages
```

### Link Header Pagination
```yaml
pagination:
  type: link
  link_header: true
```

## 🔄 Error Handling & Retries

```yaml
retry_config:
  max_attempts: 3
  initial_backoff: 1.0    # seconds
  backoff_multiplier: 2.0
  retryable_statuses: [429, 502, 503, 504]
```

## 🧪 Tested APIs

Nexus Core has been tested with these APIs:

- ✅ **Stripe** - Payment processing
- ✅ **Shopify** - E-commerce platform
- ✅ **Reddit** - Social media
- ✅ **GitHub** - Code repositories
- ✅ **JSONPlaceholder** - Testing API
- ✅ **Linear** - Project management
- ⚠️ **Salesforce** - Known compatibility issues ("working" on it!)

*Note: Some complex enterprise APIs may require additional configuration.*

See our [Alexandria](https://github.com/saturnines/Alexandria) repository for more API configurations.

## 📚 Examples

Check out the [`demo/`](demo/) directory for working examples:

- [`demo/reddit/`](demo/reddit/) - Reddit API extraction
- [`demo/shopify/`](demo/shopify/) - Shopify products with authentication
- [`demo/stripe-simple/`](demo/stripe-simple/) - Stripe customers with pagination

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   YAML Config   │───▶│  Nexus Core      │───▶│   Extracted     │
│                 │    │                  │    │   Data          │
│ • Source        │    │ • Authentication │    │                 │
│ • Auth          │    │ • Pagination     │    │ []map[string]   │
│ • Pagination    │    │ • Field Mapping  │    │ interface{}     │
│ • Field Mapping │    │ • Error Handling │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by modern ETL tools and the need for simple API integration
- Built with ❤️ for the Go community
- Special thanks to all our contributors and testers

---

**Questions?** Open an issue or start a discussion. 