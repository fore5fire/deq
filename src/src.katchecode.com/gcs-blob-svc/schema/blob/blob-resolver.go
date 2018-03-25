package blob

import (
	// "github.com/rs/zerolog/log"
	"src.katchecode.com/gcs-blob-svc/schema/datetime"
	"src.katchecode.com/gcs-blob-svc/schema/url"
)

// Resolver is a GraphQL Resolver for Blobs
type Resolver struct {
	b Blob
}

// NewResolver contsructs a new GraphQL Resolver for Blobs
func NewResolver(b Blob) *Resolver {
	return &Resolver{b}
}

// Name resolves ...
func (r Resolver) Name() string {
	return r.b.Name
}

// URL resolves ...
func (r Resolver) URL() *url.GraphQLURL {
	return url.New(r.b.URL)
}

// SignedURL resolves ...
func (r Resolver) SignedURL() (*SignedURLResolver, error) {
	var resolver *SignedURLResolver
	signedURL, err := r.b.SignedURL()
	if signedURL != nil {
		resolver = &SignedURLResolver{signedURL}
	}
	return resolver, err
}

func (r Resolver) ContentType() *string {
	return r.b.ContentType
}

func (r Resolver) CacheControl() *string {
	return r.b.CacheControl
}

func (r Resolver) CreatedAt() *datetime.GraphQLDateTime {
	return datetime.New(r.b.CreatedAt)
}

func (r Resolver) ContentEncoding() *string {
	return r.b.ContentEncoding
}

type SignedURLResolver struct {
	u *SignedURL
}

func (r *SignedURLResolver) URL() url.GraphQLURL {
	return *url.New(&r.u.URL)
}

func (r *SignedURLResolver) Expiration() datetime.GraphQLDateTime {
	return *datetime.New(&r.u.Expiration)
}
