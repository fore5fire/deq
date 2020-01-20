package backup

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	_ "gocloud.dev/blob/memblob"
)

func TestNew(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup bucket data
	dir, err := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	conn := "file://" + dir
	logger := TestLogger{t}
	version := uint64(0x0e105829f00debc3)
	latestFile := "0e105829f00debc3.deqbackup"
	err = ioutil.WriteFile(path.Join(dir, "index.deqbackup"), []byte(latestFile), 0644)
	if err != nil {
		t.Fatalf("write index file: %v", err)
	}

	// Create new backup.
	b, err := New(ctx, conn, logger)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Verify new backup's state.
	if diff := cmp.Diff(conn, b.connection); diff != "" {
		t.Errorf("verify connection string:\n%s", diff)
	}
	if diff := cmp.Diff(logger, b.debug, cmpopts.IgnoreUnexported(testing.T{})); diff != "" {
		t.Errorf("verify debug logger:\n%s", diff)
	}
	if diff := cmp.Diff(version, b.version); diff != "" {
		t.Errorf("verify start version:\n%s", diff)
	}
}

type TestStore struct {
	Step uint64
}

const dummyBackupContent = "dummy backup content\n"

func (s TestStore) Backup(w io.Writer, after uint64) (uint64, error) {
	_, err := w.Write([]byte(dummyBackupContent))
	if err != nil {
		return 0, err
	}
	return after + s.Step, nil
}

func TestRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup bucket
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("make bucket temp directory: %v", err)
	}
	defer os.RemoveAll(dir)
	conn := "file://" + dir

	store := TestStore{
		Step: 5,
	}

	backup := &Backup{
		connection: conn,
		debug:      TestLogger{t},
		version:    0x08,
	}

	// Run backup
	err = backup.Run(ctx, store)
	if err != nil {
		t.Fatalf("run backup: %v", err)
	}

	// Verify index file. Index file should have file named for version (8) +
	// store.Step (5) in hex.
	indexData, err := ioutil.ReadFile(path.Join(dir, "index.deqbackup"))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if diff := cmp.Diff("000000000000000d.deqbackup", string(indexData)); diff != "" {
		t.Errorf("verify index file content:\n%s", diff)
	}

	// Verify the backup file. The backup file should be named for the starting
	// version and contain "dummy backup content"
	backupData, err := ioutil.ReadFile(path.Join(dir, "0000000000000008.deqbackup"))
	if err != nil {
		t.Fatalf("read backup file: %v", err)
	}
	if diff := cmp.Diff(dummyBackupContent, string(backupData)); diff != "" {
		t.Fatalf("verify backup file content:\n%s", err)
	}

	// Version should now be original version (8) + store step size (5).
	if diff := cmp.Diff(uint64(0x0d), backup.version); diff != "" {
		t.Errorf("verify next version:\n%s", diff)
	}
}

func TestLoader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup bucket
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("make bucket temp directory: %v", err)
	}
	defer os.RemoveAll(dir)
	conn := "file://" + dir
	const validFileContent = "valid"
	const invalidFileContent = "invalid"
	err = ioutil.WriteFile(path.Join(dir, "0000000000000000.deqbackup"), []byte(validFileContent), 0644)
	if err != nil {
		t.Fatalf("write index file: %v", err)
	}
	err = ioutil.WriteFile(path.Join(dir, "000invalid.deqbackup"), []byte(invalidFileContent), 0644)
	if err != nil {
		t.Fatalf("write index file: %v", err)
	}
	err = ioutil.WriteFile(path.Join(dir, "aaaaaaaaaaaaaaaa.deqbackupinvalid"), []byte(invalidFileContent), 0644)
	if err != nil {
		t.Fatalf("write index file: %v", err)
	}
	err = ioutil.WriteFile(path.Join(dir, "ff0000000000000000.deqbackup"), []byte(validFileContent), 0644)
	if err != nil {
		t.Fatalf("write index file: %v", err)
	}

	backup := &Backup{
		connection: conn,
		debug:      TestLogger{t},
		version:    10,
	}

	loader, err := backup.NewLoader(ctx)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	for i := 0; ; i++ {
		r, err := loader.NextBackup(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		buf, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatalf("read backup file %d: %v", i, err)
		}
		if diff := cmp.Diff(validFileContent, string(buf)); diff != "" {
			t.Errorf("verify backup file %d content:\n%s", i, diff)
		}
	}
}

type TestLogger struct {
	TB testing.TB
}

func (l TestLogger) Printf(fmt string, a ...interface{}) {
	l.TB.Logf(fmt, a...)
}
