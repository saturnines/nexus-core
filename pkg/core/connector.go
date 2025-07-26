package core

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/saturnines/nexus-core/pkg/auth"
	"github.com/saturnines/nexus-core/pkg/config"
	"github.com/saturnines/nexus-core/pkg/errors"
	"github.com/saturnines/nexus-core/pkg/pagination"
	"github.com/saturnines/nexus-core/pkg/transport/graphql"
	"github.com/saturnines/nexus-core/pkg/transport/rest"
)

// Connector orchestrates HTTP requests and extraction.
type Connector struct {
	builder     RequestBuilder
	client      *http.Client
	extractor   Extractor
	cfg         *config.Pipeline
	authHandler auth.Handler
	factory     *pagination.Factory
}

// ConnectorOption customises Connector.
type ConnectorOption func(*Connector)

// NewConnector builds a Connector based on cfg.Source.Type.
func NewConnector(cfg *config.Pipeline, opts ...ConnectorOption) (*Connector, error) {
	transport := http.DefaultTransport
	if cfg.RetryConfig != nil {
		transport = NewRetryTransport(transport, cfg.RetryConfig)
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	var authHandler auth.Handler
	if cfg.Source.Auth != nil {
		h, err := auth.CreateHandler(cfg.Source.Auth)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrAuthentication, "auth handler")
		}
		if oauth2Auth, ok := h.(*auth.OAuth2Auth); ok {
			httpClient.Transport = auth.NewOAuth2RoundTripper(httpClient.Transport, oauth2Auth)
		} else {
			authHandler = h
		}
	}

	var builder RequestBuilder
	var extractor Extractor

	switch cfg.Source.Type {
	case config.SourceTypeREST:
		builder = rest.NewBuilder(
			cfg.Source.Endpoint,
			cfg.Source.Method,
			cfg.Source.Headers,
			cfg.Source.QueryParams,
			authHandler,
		)
		extractor = NewRestExtractor(cfg.Source.ResponseMapping)

	case config.SourceTypeGraphQL:
		g := cfg.Source.GraphQLConfig
		if g == nil {
			return nil, fmt.Errorf("graphql config missing")
		}
		builder = graphql.NewBuilder(
			g.Endpoint,
			g.Query,
			g.Variables,
			g.Headers,
			authHandler,
		)
		extractor = NewGraphQLExtractor(g)

	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Source.Type)
	}

	conn := &Connector{
		builder:     builder,
		client:      httpClient,
		extractor:   extractor,
		authHandler: authHandler,
		cfg:         cfg,
		factory:     pagination.DefaultFactory,
	}
	for _, o := range opts {
		o(conn)
	}
	return conn, nil
}

// Extract either makes a single request or paginates
func (c *Connector) Extract(ctx context.Context) ([]map[string]interface{}, error) {
	if c.cfg.Pagination == nil {
		req, err := c.builder.Build(ctx)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "build request")
		}
		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, c.handleAuthError(err)
			}
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "http do")
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, errors.WrapError(
				fmt.Errorf("API returned status %d", resp.StatusCode),
				errors.ErrHTTPResponse,
				"unexpected status code",
			)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPResponse, "read response body")
		}

		return c.extractFromBytes(body)
	}

	pager, err := c.createPager(ctx)
	if err != nil {
		return nil, errors.WrapError(err, errors.ErrPagination, "create pager")
	}

	var all []map[string]interface{}
	for {
		req, err := pager.NextRequest()
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrPagination, "next request")
		}
		if req == nil {
			break
		}
		if c.authHandler != nil {
			if err := c.authHandler.ApplyAuth(req); err != nil {
				return nil, c.handleAuthError(err)
			}
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrHTTPRequest, "http do")
		}

		bytes, err := readAndBuffer(resp)
		if err != nil {
			return nil, err
		}

		// catch non200 and non 429 here
		if resp.StatusCode != http.StatusOK {
			// map 429 → ErrPagination, everything else → ErrHTTPResponse
			errType := errors.ErrHTTPResponse
			if resp.StatusCode == http.StatusTooManyRequests {
				errType = errors.ErrPagination
			}

			return nil, errors.WrapError(
				fmt.Errorf("API returned status %d", resp.StatusCode),
				errType,
				"unexpected status code",
			)
		}

		buffered := c.createBufferedResponse(resp, bytes)
		if err := pager.UpdateState(buffered); err != nil {
			return nil, errors.WrapError(err, errors.ErrPagination, "update state")
		}

		page, err := c.extractFromBytes(bytes)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
	}

	return all, nil
}

func (c *Connector) extractFromBytes(b []byte) ([]map[string]interface{}, error) {
	if c.cfg.Source.Type == config.SourceTypeGraphQL {
		if err := errors.CheckGraphQLErrors(b); err != nil {
			return nil, err
		}
	}

	items, err := c.extractor.Items(b)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for i, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("item at index %d is not a map: %v", i, item)
		}
		mapped, err := c.extractor.Map(m)
		if err != nil {
			return nil, errors.WrapError(err, errors.ErrExtraction, fmt.Sprintf("map item at index %d", i))
		}
		results = append(results, mapped)
	}
	return results, nil
}

func readAndBuffer(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WrapError(err, errors.ErrHTTPResponse, "read body")
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return b, nil
}

func (c *Connector) createPager(ctx context.Context) (pagination.Pager, error) {
	if c.cfg.Pagination == nil {
		return nil, nil
	}

	//  handling for GraphQL cursor pagination (may need to clean up this if I decide to add more support for other types.)
	if c.cfg.Source.Type == config.SourceTypeGraphQL &&
		c.cfg.Pagination.Type == config.PaginationTypeCursor {

		gqlBuilder, ok := c.builder.(*graphql.Builder)
		if !ok {
			return nil, fmt.Errorf("expected GraphQL builder for GraphQL source")
		}

		// Parse the paths as is
		cursorPath := strings.Split(c.cfg.Pagination.CursorPath, ".")
		hasNextPath := strings.Split(c.cfg.Pagination.HasMorePath, ".")

		// Create GraphQL client wrapper
		gqlClient := graphql.NewClient(c.client)

		return graphql.NewPager(
			ctx,
			gqlBuilder,
			gqlClient,
			c.cfg.Pagination.CursorParam,
			cursorPath,
			hasNextPath,
		)
	}

	// For REST sources default use the existing factory approach
	req, err := c.builder.Build(ctx)
	if err != nil {
		return nil, err
	}

	opts := c.paginationConfigToPagerOptions()
	return c.factory.CreatePager(string(c.cfg.Pagination.Type), c.client, req, opts)
}

func (c *Connector) paginationConfigToPagerOptions() map[string]interface{} {
	p := c.cfg.Pagination
	opts := make(map[string]interface{})

	switch p.Type {
	case config.PaginationTypePage:
		opts["pageParam"] = p.PageParam
		opts["sizeParam"] = p.SizeParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalPagesPath"] = p.TotalPagesPath
		opts["startPage"] = 1
		opts["pageSize"] = p.PageSize

	case config.PaginationTypeOffset:
		opts["offsetParam"] = p.OffsetParam
		opts["sizeParam"] = p.LimitParam
		opts["hasMorePath"] = p.HasMorePath
		opts["totalCountPath"] = p.TotalCountPath
		opts["initOffset"] = 0
		opts["pageSize"] = p.OffsetIncrement

	case config.PaginationTypeCursor:
		opts["cursorParam"] = p.CursorParam
		opts["nextPath"] = p.CursorPath
	}

	return opts
}

func (c *Connector) handleAuthError(err error) error {
	var tr *auth.TokenRefreshError
	if errors.As(err, &tr) {
		return errors.WrapError(err, errors.ErrTokenExpired, "token refresh failed")
	}
	return errors.WrapError(err, errors.ErrAuthentication, "authentication failed")
}

func (c *Connector) createBufferedResponse(orig *http.Response, b []byte) *http.Response {
	return &http.Response{
		Status:           orig.Status,
		StatusCode:       orig.StatusCode,
		Proto:            orig.Proto,
		ProtoMajor:       orig.ProtoMajor,
		ProtoMinor:       orig.ProtoMinor,
		Header:           orig.Header,
		Body:             io.NopCloser(bytes.NewReader(b)),
		ContentLength:    orig.ContentLength,
		TransferEncoding: orig.TransferEncoding,
		Close:            orig.Close,
		Uncompressed:     orig.Uncompressed,
		Trailer:          orig.Trailer,
		Request:          orig.Request,
		TLS:              orig.TLS,
	}
}

// WithTimeout lets you override the HTTP client timeout.
func WithTimeout(timeout time.Duration) ConnectorOption {
	return func(c *Connector) {
		c.client.Timeout = timeout
	}
}

// WithCustomHTTPClient replaces the HTTP client entirely.
func WithCustomHTTPClient(client *http.Client) ConnectorOption {
	return func(c *Connector) {
		c.client = client
	}
}

// WithConnectorHTTPOptions applies REST HTTPClientOption(s) to the connector.
func WithConnectorHTTPOptions(options ...rest.HTTPClientOption) ConnectorOption {
	return func(c *Connector) {
		var doer rest.HTTPDoer = c.client
		for _, opt := range options {
			doer = opt(doer)
		}
		if client, ok := doer.(*http.Client); ok {
			c.client = client
		} else {
			c.client = &http.Client{
				Transport: &customRoundTripper{doer: doer},
				Timeout:   c.client.Timeout,
			}
		}
	}
}

type customRoundTripper struct {
	doer rest.HTTPDoer
}

func (rt *customRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt.doer.Do(req)
}
