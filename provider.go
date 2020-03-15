package deq

import (
	"context"
	"fmt"
	"net/url"
)

// Provider is the interface used to register a DEQ connection string protocol.
type Provider interface {
	Open(ctx context.Context, uri *url.URL) (Client, error)
}

var providers = make(map[string]Provider)

// RegisterProvider registers a provider for a uri scheme.
//
// RegisterProvider panics if a scheme is registered more than once. Don't
// include :// in scheme.
func RegisterProvider(scheme string, provider Provider) {
	if _, ok := providers[scheme]; ok {
		panic(fmt.Sprintf("attempting to re-register existing DEQ connection scheme %q", scheme))
	}
	providers[scheme] = provider
}

// Open opens a new client with the given connection string.
func Open(ctx context.Context, uri string) (Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("parse connection URI: %v", err)
	}

	p, ok := providers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("parse connection URI: scheme %q is not supported", u.Scheme)
	}

	db, err := p.Open(ctx, u)
	if err != nil {
		return nil, err
	}
	return db, nil
}
