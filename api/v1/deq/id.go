package deq

import (
	"encoding/binary"
	"time"
)

// DeprecatedTimeFromID returns the time component of an EventV0's ID
func DeprecatedTimeFromID(id []byte) time.Time {
	return time.Unix(0, int64(binary.BigEndian.Uint64(id)>>16)*int64(time.Millisecond))
}
