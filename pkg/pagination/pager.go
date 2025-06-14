package pagination

import "net/http"

// HTTPDoer can perform HTTP requests.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Pager drives one pagination strategy.
type Pager interface {
	NextRequest() (*http.Request, error)
	UpdateState(resp *http.Response) error
}
