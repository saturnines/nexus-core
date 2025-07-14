# Nexus Core

Universal API data extractor built in Go. Configure any REST/GraphQL API with YAML and extract all data automatically.

## Quick Start

**Simple API (no auth needed):**
```yaml
name: jsonplaceholder-posts
source:
  type: rest
  endpoint: https://jsonplaceholder.typicode.com/posts
  response_mapping:
    fields:
      - name: id
        path: id
      - name: title
        path: title
      - name: author_id
        path: userId
```

**Complex API (auth + pagination):**
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
pagination:
  type: cursor
  cursor_param: starting_after
  cursor_path: data.-1.id
```

```go
connector, _ := core.NewConnector(config)
results, _ := connector.Extract(context.Background())
fmt.Printf("Extracted %d records\n", len(results))
```

## Features

- **Authentication**: Basic, Bearer, API Key, OAuth2 with refresh
- **Pagination**: Cursor, offset, page-based, link headers
- **Data Mapping**: JSONPath field extraction
- **Error Handling**: Automatic retries with backoff
- **Multi-Protocol**: REST and GraphQL support

## Installation

```bash
go get github.com/saturnines/nexus-core
```

## Status

Works pretty well for most APIs I've thrown at it. Still adding stuff when I find edge cases that break it.

Feel free to try it out or submit PRs if you find bugs.

TODO List:
- CI/CD for integration tests
- Threadsafe pagination
- GraphQL test coverage

*Personal project for learning Go and solving repetitive API integration work.*