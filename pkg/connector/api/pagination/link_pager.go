package pagination

import (
	"net/http"
	"net/url"
	"strings"
)

// LinkPager drives Link based pagination in a REST client.
type LinkPager struct {
	Client  HTTPDoer
	BaseReq *http.Request
	nextURL string
}

// NewLinkPager starts at the BaseReq’s URL.
func NewLinkPager(client HTTPDoer, req *http.Request) *LinkPager {
	return &LinkPager{
		Client:  client,
		BaseReq: req,
		nextURL: req.URL.String(),
	}
}

// NextRequest returns the next *http.Request, or nil when there’s no next link.
func (p *LinkPager) NextRequest() (*http.Request, error) {
	if p.nextURL == "" {
		return nil, nil
	}

	raw := p.nextURL
	base := p.BaseReq.URL

	// should handle scheme relative URLs: "//host/path" → "http://host/path"
	if strings.HasPrefix(raw, "//") {
		raw = base.Scheme + ":" + raw
	}

	// resolve if it's a true relative path.
	var u *url.URL
	var err error
	if strings.HasPrefix(raw, "/") {
		u, err = base.Parse(raw)
		if err != nil {
			return nil, err
		}
	} else {
		// Otherwise parse as absolute
		u, err = url.Parse(raw)
		if err != nil {
			return nil, err
		}
		if !u.IsAbs() {
			// still resolve it against base if parsing produced something not absolute
			u = base.ResolveReference(u)
		}
	}

	// Build a fresh request reusing headers and context.
	req := p.BaseReq.Clone(p.BaseReq.Context())
	req.URL = u
	return req, nil
}

// UpdateState reads the Link header and sets p.nextURL to the next link or empty
func (p *LinkPager) UpdateState(resp *http.Response) error {
	header := resp.Header.Get("Link")
	links := parseLinkHeader(header)
	p.nextURL = links["next"]
	return nil
}

// parseLinkHeader splits “<url>; rel=\"next\", <url2>; rel=\"last\"”
// into a map: { "next": "url", "last": "url2" }.
// Ignores segments without a rel= value.
func parseLinkHeader(header string) map[string]string {
	parts := strings.Split(header, ",")
	links := make(map[string]string, len(parts))
	for _, part := range parts {
		seg := strings.Split(strings.TrimSpace(part), ";")
		if len(seg) < 2 {
			continue
		}
		rawURL := strings.Trim(seg[0], "<> ")
		var rel string
		for _, param := range seg[1:] {
			pair := strings.SplitN(strings.TrimSpace(param), "=", 2)
			if len(pair) != 2 {
				continue
			}
			if pair[0] == "rel" {
				rel = strings.Trim(pair[1], `"`)
			}
		}
		if rel != "" {
			links[rel] = rawURL
		}
	}
	return links
}
