package blob

import (
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"net/url"
	"time"
)

type Blob struct {
	Name            string
	URL             *url.URL
	ContentType     *string
	CacheControl    *string
	CreatedAt       *time.Time
	ContentEncoding *string
	ContentLength   *int64
	bucket          *string
	gcsUser         string
	gcsPrivateKey   string
}

func New(attrs storage.ObjectAttrs, gcsUser string, gcsPrivateKey string) Blob {

	u, err := url.Parse(attrs.MediaLink)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error parsing gcs object media link")
	}

	return Blob{
		Name:            attrs.Name,
		URL:             u,
		ContentType:     &attrs.ContentType,
		CacheControl:    &attrs.CacheControl,
		CreatedAt:       &attrs.Created,
		ContentEncoding: &attrs.ContentEncoding,
		ContentLength:   &attrs.Size,
		bucket:          &attrs.Bucket,
		gcsUser:         gcsUser,
		gcsPrivateKey:   gcsPrivateKey,
	}
}

type SignedURL struct {
	URL        url.URL
	Expiration time.Time
}

// SignedURL returns a pre-authorized url for viewing or editing blobs
func (b *Blob) SignedURL() (*SignedURL, error) {

	expiration := time.Now().Add(time.Second * 600)

	signed, err := storage.SignedURL(*b.bucket, b.Name, &storage.SignedURLOptions{
		GoogleAccessID: b.gcsUser,
		PrivateKey:     []byte(b.gcsPrivateKey),
		Method:         "GET",
		Expires:        expiration,
	})
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error creating signed gcs url")
		return nil, err
	}

	log.Debug().
		Str("url", signed).
		Msg("Successfully signed url")

	u, err := url.Parse(signed)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error parsing signed gcs url")
		return nil, err
	}

	return &SignedURL{
		URL:        *u,
		Expiration: expiration,
	}, nil
}
