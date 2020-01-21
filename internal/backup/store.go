package backup

import "io"

type Store interface {
	Backup(w io.Writer, after uint64) (uint64, error)
}
