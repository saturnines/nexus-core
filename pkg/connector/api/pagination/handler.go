package pagination

import "net/http"

// Handler handles a paged API.
type Handler interface {
	// NextRequest returns the next HTTP request.
	NextRequest() (*http.Request, error)
	// Consume reads the response, advances state, and
	// reports if there’s another page.
	Consume(resp *http.Response) (hasMore bool, err error)
}
