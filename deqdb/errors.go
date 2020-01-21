package deqdb

import (
	"github.com/dgraph-io/badger/v2"
	"gitlab.com/katcheCode/deq/deqerr"
)

func errFromBadger(err error) error {
	code := deqerr.Internal
	switch err {
	case badger.ErrKeyNotFound:
		code = deqerr.NotFound
	case badger.ErrConflict, badger.ErrRetry:
		code = deqerr.Unavailable
	case badger.ErrEmptyKey, badger.ErrWindowsNotSupported:
		code = deqerr.Invalid
	}

	return deqerr.Wrap(code, err)
}
