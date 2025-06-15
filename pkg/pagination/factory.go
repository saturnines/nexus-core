package pagination

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"github.com/saturnines/nexus-core/pkg/errors"
)

// Factory holds a registry of Pager creators.
type Factory struct {
	mu       sync.RWMutex
	registry map[string]Creator
}

// NewFactory returns an empty Factory.
func NewFactory() *Factory {
	return &Factory{
		registry: make(map[string]Creator),
	}
}

// RegisterPager adds a new Pager creator.
// It errors if something is already registered
func (f *Factory) RegisterPager(kind string, creator Creator) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.registry[kind]; exists {
		return errors.WrapError(
			fmt.Errorf("pager %q already registered", kind),
			errors.ErrConfiguration,
			"register pager",
		)
	}
	f.registry[kind] = creator
	return nil
}

// CreatePager looks up and invokes a creator.
// It returns a wrapped error on missing kind (i.e, offset, cursor, token) or bad options.
func (f *Factory) CreatePager(
	kind string,
	client HTTPDoer,
	req *http.Request,
	opts map[string]interface{},
) (Pager, error) {
	f.mu.RLock()
	creator, ok := f.registry[kind]
	f.mu.RUnlock()
	if !ok {
		return nil, errors.WrapError(
			fmt.Errorf("unsupported pager type: %s", kind),
			errors.ErrConfiguration,
			"create pager",
		)
	}
	pager, err := creator(client, req, opts)
	if err != nil {
		return nil, errors.WrapError(
			err,
			errors.ErrConfiguration,
			fmt.Sprintf("creating %q pager", kind),
		)
	}
	return pager, nil
}

// GetAvailablePagers returns a sorted list of registered kinds.
func (f *Factory) GetAvailablePagers() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	kinds := make([]string, 0, len(f.registry))
	for kind := range f.registry {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// ValidatePagerOptions tries to build a Pager with dummy values.
// It returns any configuration error from the creator.
func (f *Factory) ValidatePagerOptions(
	kind string,
	opts map[string]interface{},
) error {
	f.mu.RLock()
	creator, ok := f.registry[kind]
	f.mu.RUnlock()
	if !ok {
		return errors.WrapError(
			fmt.Errorf("unsupported pager type: %s", kind),
			errors.ErrConfiguration,
			"validate pager",
		)
	}
	dummyClient := &noOpHTTPDoer{}
	stubReq := &http.Request{URL: &url.URL{}}
	_, err := creator(dummyClient, stubReq, opts)
	return err
}

// noOpHTTPDoer satisfies HTTPDoer without side effects.
type noOpHTTPDoer struct{}

func (d *noOpHTTPDoer) Do(*http.Request) (*http.Response, error) {
	return &http.Response{}, nil
}

// DefaultFactory is the global registry.
var DefaultFactory = NewFactory()

func init() {
	_ = DefaultFactory.RegisterPager("cursor", cursorCreator)
	_ = DefaultFactory.RegisterPager("page", pageCreator)
	_ = DefaultFactory.RegisterPager("offset", offsetCreator)
	_ = DefaultFactory.RegisterPager("link", linkCreator)
}
