package deq

import (
	"path"
	"testing"
)

// func TestVersionMismatch(t *testing.T) {
// 	_, err := Open(Options{
// 		Dir: path.Join("testdata", "testdb"),
// 	})
// 	if err != ErrVersionMismatch {
// 		t.Fatal(err)
// 	}
// }

func TestUpgradeV1_0_0(t *testing.T) {

	// for {
	// 	n, err := source.Read(buf)
	// 	if err != nil && err != io.EOF {
	// 		return err
	// 	}
	// 	if n == 0 {
	// 		break
	// 	}

	// 	if _, err := destination.Write(buf[:n]); err != nil {
	// 		return err
	// 	}
	// }
	db, err := Open(Options{
		Dir:             path.Join("testdata", "testdb"),
		UpgradeIfNeeded: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// channel := db.Channel("test-upgrade-v1_0_0", "test-topic")

}
