package eventdb

import (
	"bytes"
	"testing"
	"time"
)

// Should sort CreateTime, and keep topics seperate.
func TestKeyOrder(t *testing.T) {
	// Round to remove monotonic clock information, it gets removed by
	// serialization anyway
	now := time.Now().Round(0)

	keys := []Key{
		&EventKey{
			Topic:      "a",
			CreateTime: time.Unix(0, 1000),
			ID:         "a",
		},
		&EventKey{
			Topic:      "a",
			CreateTime: time.Unix(0, 1000),
			ID:         "b",
		},
		&EventKey{
			Topic:      "a",
			CreateTime: now,
			ID:         "a",
		},
		&EventKey{
			Topic:      "a",
			CreateTime: now,
			ID:         "b",
		},
		&EventKey{
			Topic:      "a",
			CreateTime: now.Add(time.Second),
			ID:         "a",
		},
		&EventKey{
			Topic:      "b",
			CreateTime: time.Unix(0, 1),
			ID:         "a",
		},
		&EventKey{
			Topic:      "b",
			CreateTime: now.Add(time.Second * -1),
			ID:         "a",
		},
		&EventKey{
			Topic:      "b",
			CreateTime: now,
			ID:         "a",
		},
		&EventKey{
			Topic:      "b",
			CreateTime: now,
			ID:         "b",
		},
		&EventKey{
			Topic:      "b",
			CreateTime: now.Add(time.Nanosecond),
			ID:         "a",
		},
	}

	slices := make([][]byte, len(keys))
	var err error
	for i, key := range keys {
		slices[i], err = key.Marshal(nil)
		if err != nil {
			t.Fatalf("marshal keys[%d] %v: %v", i, key, err)
		}
	}

	for i := 1; i < len(slices); i++ {
		if bytes.Compare(slices[i-1], slices[i]) != -1 {
			t.Errorf("incorrect order:\n%d %v %v\n%d %v %v", i-1, keys[i-1], slices[i-1], i, keys[i], slices[i])
		}
	}
}
