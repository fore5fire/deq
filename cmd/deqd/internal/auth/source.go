package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	jose "gopkg.in/square/go-jose.v2"
)

type KeySource interface {
	KeySet(context.Context) (*jose.JSONWebKeySet, error)
}

type httpKeySource struct {
	jwksURL      string
	cached       *jose.JSONWebKeySet
	cacheTimeout time.Time
	client       *http.Client
}

// NewHTTPKeySource creates a KeySource that provides keys loaded from a URL.
//
// client making a GET request made to jwksURL is expected to return a JSON Web Key Set. if client
// is nil, http.DefaultClient is used. The KeySource
func NewHTTPKeySource(jwksURL string, client *http.Client) KeySource {
	if client == nil {
		client = http.DefaultClient
	}
	return &httpKeySource{
		jwksURL: jwksURL,
		client:  client,
	}
}

func (p *httpKeySource) KeySet(ctx context.Context) (*jose.JSONWebKeySet, error) {
	// If we have the keys cached and the cache hasn't expired, just return them.
	if !p.cacheTimeout.IsZero() && p.cacheTimeout.After(time.Now()) {
		return p.cached, nil
	}

	// Cache miss - setup a request to get the keys.
	req, err := http.NewRequestWithContext(ctx, "GET", p.jwksURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Make the request.
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("make request: %w", err)
	}

	// Ensure we don't read more than the content length
	resp.Body = http.MaxBytesReader(nil, resp.Body, resp.ContentLength)

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		// Read the response body for an error message.
		body := make([]byte, resp.ContentLength)
		_, err = io.ReadFull(req.Body, body)
		if err != nil {
			return nil, fmt.Errorf("parse response: %q", resp.Status)
		}
		return nil, fmt.Errorf("parse response: %q %q", resp.Status, body)
	}

	// The response is OK - parse the body
	jwks := new(jose.JSONWebKeySet)
	err = json.NewDecoder(resp.Body).Decode(jwks)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return jwks, nil
}

type staticSource struct {
	jwks *jose.JSONWebKeySet
}

func NewStaticKeySource(jwks *jose.JSONWebKeySet) KeySource {
	if jwks == nil {
		panic("argument jwks is required")
	}
	return &staticSource{
		jwks: jwks,
	}
}

func (p *staticSource) KeySet(context.Context) (*jose.JSONWebKeySet, error) {
	return p.jwks, nil
}

type JWKSFuncSource func(context.Context) (*jose.JSONWebKeySet, error)

type funcSource struct {
	Func JWKSFuncSource
}

func NewFuncKeySource(jwksFunc JWKSFuncSource) KeySource {
	if jwksFunc == nil {
		panic("argument jwksFunc is required")
	}
	return &funcSource{
		Func: jwksFunc,
	}
}

func (p *funcSource) KeySet(ctx context.Context) (*jose.JSONWebKeySet, error) {
	return p.Func(ctx)
}
