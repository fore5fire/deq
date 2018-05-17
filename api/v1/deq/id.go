package deq

import (
	"encoding/binary"
	"time"
)

// TimeFromID returns the time component of the DEQ id
func TimeFromID(id []byte) time.Time {
	return time.Unix(0, int64(binary.BigEndian.Uint64(id)>>16)*int64(time.Millisecond))
}
