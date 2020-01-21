package backup

import (
	"context"
	"fmt"

	"gitlab.com/katcheCode/deq/internal/backup"
	"gitlab.com/katcheCode/deq/internal/log"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

type Backup struct {
	version    uint64
	connection string
	debug      log.Logger
}

func New(ctx context.Context, connection string, debug log.Logger) (*Backup, error) {

	if debug == nil {
		debug = log.NoOpLogger{}
	}

	b := &Backup{
		connection: connection,
		debug:      debug,
	}
	bucket, err := blob.OpenBucket(ctx, b.connection)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Read current version from index if it exists
	versionBuf, err := bucket.ReadAll(ctx, backup.IndexName)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return b, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read index: %v", err)
	}
	versionStr := string(versionBuf)

	version, err := backup.VersionForName(versionStr)
	if err != nil {
		return nil, fmt.Errorf("parse index content: %v", err)
	}

	b.version = version

	return b, nil
}

func (b *Backup) Run(ctx context.Context, store backup.Store) error {
	bucket, err := blob.OpenBucket(ctx, b.connection)
	if err != nil {
		return fmt.Errorf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Write to file named for its start version.
	objectName := backup.NameForVersion(b.version)
	w, err := bucket.NewWriter(ctx, objectName, &blob.WriterOptions{
		ContentDisposition: "attachment",
	})
	if err != nil {
		return fmt.Errorf("open object %q in bucket: %v", objectName, err)
	}
	newVersion, err := store.Backup(w, b.version)
	if err != ctx.Err() {
		return fmt.Errorf("write backup %q: %v", objectName, err)
	}
	b.debug.Printf("got next latest version %d", newVersion)
	err = w.Close()
	if err != nil {
		return fmt.Errorf("close backup %q: %v", objectName, err)
	}
	b.debug.Printf("backup object %q written", objectName)

	if newVersion == b.version {
		b.debug.Printf("no index update needed")
		return nil
	}

	// Once backup is closed, update the index
	nextObjectName := backup.NameForVersion(newVersion)
	err = bucket.WriteAll(ctx, backup.IndexName, []byte(nextObjectName), nil)
	if err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	b.version = newVersion

	b.debug.Printf("%q written to index file %q", nextObjectName, backup.IndexName)

	return nil
}
