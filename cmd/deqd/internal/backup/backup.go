package backup

import (
	"context"
	"fmt"
	"log"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

const backupExtension = ".deqbackup"
const indexName = "index" + backupExtension

type Backup struct {
	version    uint64
	connection string
	debug      Logger
}

func New(ctx context.Context, connection string, debug Logger) (*Backup, error) {

	if debug == nil {
		debug = noopLogger{}
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

	// Read current version from index
	versionBuf, err := bucket.ReadAll(ctx, indexName)
	versionStr := string(versionBuf)

	version, err := versionForName(versionStr)
	if err != nil {
		log.Printf("parse \".deqbackup\" object %q: %v - skipping", versionStr, err)
	}

	b.version = version

	return b, nil
}

func (b *Backup) Run(ctx context.Context, store Store) error {
	bucket, err := blob.OpenBucket(ctx, b.connection)
	if err != nil {
		return fmt.Errorf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Write to file named for its start version.
	objectName := nameForVersion(b.version)
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
	err = w.Close()
	if err != nil {
		return fmt.Errorf("close backup %q: %v", objectName, err)
	}

	b.debug.Printf("backup object %q written", objectName)

	// Once backup is closed, update the index
	nextObjectName := nameForVersion(newVersion)
	err = bucket.WriteAll(ctx, indexName, []byte(nextObjectName), nil)
	if err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	b.version = newVersion

	b.debug.Printf("%q written to index file %q", nextObjectName, indexName)

	return nil
}
