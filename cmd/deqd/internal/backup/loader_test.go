package backup

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
)

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
