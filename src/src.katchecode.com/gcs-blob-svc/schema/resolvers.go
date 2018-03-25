package schema

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
	"net/url"
	"src.katchecode.com/gcs-blob-svc/schema/blob"
	"src.katchecode.com/gcs-blob-svc/schema/datetime"
	graphqlurl "src.katchecode.com/gcs-blob-svc/schema/url"
	"time"
)

type ListBlobsFilter struct {
	Delimiter *string
	Prefix    *string
	Cursor    *struct {
		PageToken *string
		Previous  *bool
		Limit     *int
	}
}

type BlobCursor struct {
	Blobs     []blob.Blob
	PageToken string
}

type BlobCursorResolver struct {
	b *BlobCursor
}

func (r *BlobCursorResolver) Blobs() []blob.Resolver {
	res := make([]blob.Resolver, len(r.b.Blobs))
	for i, item := range r.b.Blobs {
		res[i] = *blob.NewResolver(item)
	}

	return res
}

func (r *BlobCursorResolver) PageToken() string {
	return r.b.PageToken
}

// Resolver groups GraphQL resolvers for the root types (Query, Mutation, Subscription)
type Resolver struct {
	Bucket        string
	Prefix        string
	GCSUser       string
	GCSPrivateKey string
}

func NewResolver(bucket string, prefix string, gcsUser string, gcsPrivateKey string) *Resolver {
	return &Resolver{
		Bucket:        bucket,
		Prefix:        prefix,
		GCSUser:       gcsUser,
		GCSPrivateKey: gcsPrivateKey,
	}
}

var defaultTo = "world!"

func (r *Resolver) Blobs(ctx context.Context, args struct{ Filter *ListBlobsFilter }) (*BlobCursorResolver, error) {
	log.Debug().
		Interface("args", args).
		Msg("Getting Blobs")

	queryOptions := storage.Query{}
	pageToken := ""
	pageSize := 30

	if args.Filter != nil {

		if args.Filter.Delimiter != nil {
			queryOptions.Delimiter = *args.Filter.Delimiter
		}

		if args.Filter.Prefix != nil {
			queryOptions.Prefix = *args.Filter.Delimiter
		}

		if args.Filter.Cursor != nil {

			if args.Filter.Cursor.PageToken == nil {
				pageToken = *args.Filter.Cursor.PageToken
			}

			if args.Filter.Cursor.Previous == nil {

			}

			if args.Filter.Cursor.Limit == nil {
				pageSize = *args.Filter.Cursor.Limit
			}
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error creating gcs client")
		return nil, err
	}

	bucket := client.Bucket(r.Bucket)
	objects := bucket.Objects(ctx, &queryOptions)
	pages := iterator.NewPager(objects, pageSize, pageToken)

	objectAttrs := make([]*storage.ObjectAttrs, 0, pageSize)

	nextPageToken, err := pages.NextPage(&objectAttrs)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error getting objects")
		return nil, err
	}

	blobCursor := &BlobCursor{
		PageToken: nextPageToken,
		Blobs:     make([]blob.Blob, len(objectAttrs)),
	}

	for i, item := range objectAttrs {
		if item == nil {
			blobCursor.Blobs = blobCursor.Blobs[:i]
			break
		}
		newBlob := blob.New(*item, r.GCSUser, r.GCSPrivateKey)
		blobCursor.Blobs[i] = newBlob
	}

	return &BlobCursorResolver{blobCursor}, nil
}

func (r *Resolver) Blob(ctx context.Context, args struct{ Name string }) (*blob.Resolver, error) {
	log.Debug().
		Interface("args", args).
		Msg("Getting Blobs")

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error creating gcs client")
		return nil, err
	}

	bucket := client.Bucket(r.Bucket)
	object := bucket.Object(r.Prefix + args.Name)
	attrs, err := object.Attrs(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error getting object attributes")
		return nil, err
	}

	if attrs == nil {
		return nil, nil
	}

	b := blob.New(*attrs, r.GCSUser, r.GCSPrivateKey)

	return blob.NewResolver(b), nil
}

type BlobInput struct {
	Name            string
	ContentType     *string
	CacheControl    *string
	ContentEncoding *string
}

func (r *Resolver) CreateBlobUploadURL(ctx context.Context, args struct{ Input BlobInput }) (*UploadURLResolver, error) {
	log.Debug().
		Interface("args", args).
		Msg("Creating Blob Upload URL")

	expiration := time.Now().Add(time.Second * 600)

	options := storage.SignedURLOptions{
		GoogleAccessID: r.GCSUser,
		PrivateKey:     []byte(r.GCSPrivateKey),
		Method:         "POST",
		Expires:        expiration,
	}

	if args.Input.ContentType != nil {
		options.ContentType = *args.Input.ContentType
	}

	signed, err := storage.SignedURL(r.Bucket, args.Input.Name, &options)

	uploadURL, err := url.Parse(signed)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error parsing signed upload url")
		return nil, err
	}

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error creating signed gcs url")
		return nil, err
	}

	log.Debug().
		Str("url", signed).
		Msg("Successfully signed url")

	return &UploadURLResolver{uploadURL, &expiration}, nil
}

type UploadURLResolver struct {
	url        *url.URL
	expiration *time.Time
}

func (r UploadURLResolver) URL() graphqlurl.GraphQLURL {
	return *graphqlurl.New(r.url)
}

func (r UploadURLResolver) Expiration() *datetime.GraphQLDateTime {
	return datetime.New(r.expiration)
}
