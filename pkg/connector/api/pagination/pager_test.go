// pkg/pagination/pager_test.go
package pagination

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Helpers

func assertQueryParam(t *testing.T, req *http.Request, key, want string) {
	t.Helper()
	if got := req.URL.Query().Get(key); got != want {
		t.Fatalf("expected query %q=%q, got %q", key, want, got)
	}
}

func assertNextRequestNil(t *testing.T, req *http.Request, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if req != nil {
		t.Fatalf("expected nil request, got %v", req.URL.String())
	}
}

func makeResponse(body string, status int) *http.Response {
	rec := httptest.NewRecorder()
	rec.Code = status
	rec.Body.WriteString(body)
	return rec.Result()
}

// CursorPager tests

func TestCursorPager(t *testing.T) {
	baseReq, _ := http.NewRequest("GET", "http://example.com/items", nil)
	p := NewCursorPager(nil, baseReq, "cursor", "next")

	// initial request: no cursor param
	req1, err := p.NextRequest()
	if err != nil {
		t.Fatal(err)
	}
	if got := req1.URL.Query().Get("cursor"); got != "" {
		t.Errorf("initial: expected empty cursor, got %q", got)
	}

	// simulate a page with next cursor
	resp := makeResponse(`{"next":"abc123"}`, 200)
	if err := p.UpdateState(resp); err != nil {
		t.Fatal(err)
	}
	req2, err := p.NextRequest()
	if err != nil {
		t.Fatal(err)
	}
	assertQueryParam(t, req2, "cursor", "abc123")

	// simulate last page: missing field → no more pages
	resp2 := makeResponse(`{}`, 200)
	if err := p.UpdateState(resp2); err != nil {
		t.Fatal(err)
	}
	req3, err := p.NextRequest()
	assertNextRequestNil(t, req3, err)

	// simulate error status
	bad := makeResponse(`{"next":"x"}`, 500)
	err = p.UpdateState(bad)
	if err == nil || !strings.Contains(err.Error(), "bad status") {
		t.Errorf("expected bad status error, got %v", err)
	}
}

// PagePager tests

func TestPagePager(t *testing.T) {
	baseReq, _ := http.NewRequest("GET", "http://example.com/items", nil)
	p := NewPagePager(nil, baseReq, "page", "size", "has_more", 1, 50)

	// 1st request: page=1
	req1, err := p.NextRequest()
	if err != nil {
		t.Fatal(err)
	}
	assertQueryParam(t, req1, "page", "1")
	assertQueryParam(t, req1, "size", "50")

	// simulate more pages
	resp := makeResponse(`{"has_more":true}`, 200)
	if err := p.UpdateState(resp); err != nil {
		t.Fatal(err)
	}
	req2, _ := p.NextRequest()
	assertQueryParam(t, req2, "page", "2")

	// simulate last page
	resp2 := makeResponse(`{"has_more":false}`, 200)
	if err := p.UpdateState(resp2); err != nil {
		t.Fatal(err)
	}
	req3, err := p.NextRequest()
	assertNextRequestNil(t, req3, err)
}

// OffsetPager tests

func TestOffsetPager(t *testing.T) {
	baseReq, _ := http.NewRequest("GET", "http://example.com/items", nil)
	p := NewOffsetPager(nil, baseReq, "offset", "limit", "has_more", 0, 25)

	// 1st request: offset=0, limit=25
	req1, err := p.NextRequest()
	if err != nil {
		t.Fatal(err)
	}
	assertQueryParam(t, req1, "offset", "0")
	assertQueryParam(t, req1, "limit", "25")

	// simulate more pages
	resp := makeResponse(`{"has_more":true}`, 200)
	if err := p.UpdateState(resp); err != nil {
		t.Fatal(err)
	}
	req2, _ := p.NextRequest()
	assertQueryParam(t, req2, "offset", "25")

	// simulate last page
	resp2 := makeResponse(`{"has_more":false}`, 200)
	if err := p.UpdateState(resp2); err != nil {
		t.Fatal(err)
	}
	req3, err := p.NextRequest()
	assertNextRequestNil(t, req3, err)
}

// LinkPager tests

func TestLinkPager(t *testing.T) {
	// initial URL
	baseReq, _ := http.NewRequest("GET", "http://example.com/items", nil)
	p := NewLinkPager(nil, baseReq)

	// initial next request should use base URL
	req1, err := p.NextRequest()
	if err != nil {
		t.Fatal(err)
	}
	if req1.URL.String() != "http://example.com/items" {
		t.Errorf("expected initial URL unchanged, got %s", req1.URL)
	}

	// simulate a Link header
	resp := httptest.NewRecorder()
	resp.Header().Set("Link", `<http://api.example.com/page2>; rel="next", <http://api.example.com/last>; rel="last"`)
	resp.WriteHeader(200)
	res := resp.Result()

	if err := p.UpdateState(res); err != nil {
		t.Fatal(err)
	}

	req2, _ := p.NextRequest()
	if req2.URL.String() != "http://api.example.com/page2" {
		t.Errorf("expected next URL from Link header, got %s", req2.URL)
	}

	// simulate no next link
	resp2 := httptest.NewRecorder()
	resp2.Header().Set("Link", `<http://api.example.com/last>; rel="last"`)
	resp2.WriteHeader(200)
	res2 := resp2.Result()

	if err := p.UpdateState(res2); err != nil {
		t.Fatal(err)
	}
	req3, err := p.NextRequest()
	assertNextRequestNil(t, req3, err)
}

// Example: test numeric field via a lookupInt helper
func TestLookupInt(t *testing.T) {
	body := `{"meta":{"count": 42}}`
	resp := makeResponse(body, 200)
	m, err := parseBody(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := lookupInt(m, "meta.count")
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}

// lookupInt for stdlib map[string]interface{}
func lookupInt(body map[string]interface{}, path string) (int, error) {
	parts := strings.Split(path, ".")
	var cur interface{} = body
	for _, p := range parts {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("lookupInt: %q not object", p)
		}
		cur, ok = m[p]
		if !ok {
			return 0, fmt.Errorf("lookupInt: missing %q", p)
		}
	}
	// JSON numbers are float64
	switch v := cur.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	default:
		return 0, fmt.Errorf("lookupInt: %q not a number", path)
	}
}
