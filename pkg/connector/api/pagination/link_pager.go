package pagination

import (
	"net/http"
	"net/url"
	"strings"
)

// LinkPager for Link heading.
type LinkPager struct {
	Client  HTTPDoer
	BaseReq *http.Request
	nextURL string
}

// NewLinkPager builds a LinkPager.
func NewLinkPager(client HTTPDoer, req *http.Request) *LinkPager {
	return &LinkPager{Client: client, BaseReq: req, nextURL: req.URL.String()}
}

// NextRequest returns the next request or nil when done.
func (p *LinkPager) NextRequest() (*http.Request, error) {
	if p.nextURL == "" {
		return nil, nil
	}

	var u *url.URL
	var err error

	// Handle relative URLs by resolving against base URL
	if strings.HasPrefix(p.nextURL, "/") {
		u, err = p.BaseReq.URL.Parse(p.nextURL)
	} else {
		u, err = url.Parse(p.nextURL)
	}

	if err != nil {
		return nil, err
	}
	req := p.BaseReq.Clone(p.BaseReq.Context())
	req.URL = u
	return req, nil
}

// UpdateState parses Link header and saves next URL.
func (p *LinkPager) UpdateState(resp *http.Response) error {
	header := resp.Header.Get("Link")
	links := parseLinkHeader(header)
	p.nextURL = links["next"]
	return nil
}

func parseLinkHeader(header string) map[string]string {
	parts := strings.Split(header, ",")
	links := make(map[string]string, len(parts))
	for _, part := range parts {
		seg := strings.Split(strings.TrimSpace(part), ";")
		if len(seg) < 2 {
			continue
		}
		urlPart := strings.Trim(seg[0], "<> ")
		var rel string
		for _, param := range seg[1:] {
			p := strings.SplitN(strings.TrimSpace(param), "=", 2)
			if len(p) != 2 {
				continue
			}
			if p[0] == "rel" {
				rel = strings.Trim(p[1], `"`)
			}
		}
		if rel != "" {
			links[rel] = urlPart
		}
	}
	return links
}
