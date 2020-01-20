package backup

import (
	"context"
	"fmt"
	"io"
	"log"

	"gitlab.com/katcheCode/deq/deqdb"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

type loader struct {
	bucket *blob.Bucket
	it     *blob.ListIterator
	closed bool
}

func (b *Backup) NewLoader(ctx context.Context) (deqdb.Loader, error) {

	bucket, err := blob.OpenBucket(ctx, b.connection)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %v", err)
	}

	return &loader{
		bucket: bucket,
		it:     bucket.List(nil),
	}, nil
}

func (l *loader) NextBackup(ctx context.Context) (io.ReadCloser, error) {
	if l.closed {
		return nil, io.EOF
	}
	for {
		obj, err := l.it.Next(ctx)
		if err == io.EOF {
			l.bucket.Close()
			l.closed = true
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("list backups: %v", err)
		}
		if obj.Key == "index.deqbackup" {
			continue
		}
		_, err = versionForName(obj.Key)
		if err != nil {
			log.Printf("list backups: verify name for object %q: %v - skipping", obj.Key, err)
			continue
		}
		return l.bucket.NewReader(ctx, obj.Key, nil)
	}
}
