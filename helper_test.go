package deq

import (
	"io/ioutil"
	"os"
)

func newTestDB() (*Store, func()) {
	dir, err := ioutil.TempDir("", "test-pub")
	if err != nil {
		panic("create temp dir: " + err.Error())
	}

	db, err := Open(Options{
		Dir: dir,
	})
	if err != nil {
		panic("open db: " + err.Error())
	}

	return db, func() {
		os.RemoveAll(dir)
	}
}
