package data

import (
	"bytes"
	"testing"
	"time"
)

func TestMarshalEventTimeKey(t *testing.T) {
	expected := EventTimeKey{
		Topic: "abc",
		ID:    "def",
	}
	buf, err := expected.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf[0] != EventTimeTag {
		t.Errorf("expected serialized prefix %d, got %d", EventTimeTag, buf[0])
	}

	var unmarshaled EventTimeKey
	err = UnmarshalTo(buf, &unmarshaled)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expected != unmarshaled {
		t.Errorf("expected: %v, got: %v", expected, unmarshaled)
	}
}

func TestMarshalEventKey(t *testing.T) {
	expected := EventKey{
		Topic: "abc",
		// Round to remove monotonic clock information (which won't get serilalized)
		CreateTime: time.Now().Round(0),
		ID:         "def",
	}
	buf, err := expected.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf[0] != EventTag {
		t.Errorf("expected serialized prefix %d, got %d", EventTag, buf[0])
	}

	var unmarshaled EventKey
	err = UnmarshalTo(buf, &unmarshaled)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expected != unmarshaled {
		t.Errorf("expected: %v, got: %v", expected, unmarshaled)
	}

	expectedPrefix := buf[:len(expected.Topic)+3]
	prefix, err := EventPrefix(expected.Topic)
	if err != nil {
		t.Fatalf("marshal prefix: %v", err)
	}
	if !bytes.Equal(expectedPrefix, prefix) {
		t.Errorf("marshal prefix: expected %v, got %v", expectedPrefix, prefix)
	}
}

func TestMarshalChannelKey(t *testing.T) {
	expected := ChannelKey{
		Channel: "123",
		Topic:   "abc",
		ID:      "def",
	}
	buf, err := expected.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf[0] != ChannelTag {
		t.Errorf("expected serialized prefix %d, got %d", ChannelTag, buf[0])
	}

	var unmarshaled ChannelKey
	err = UnmarshalTo(buf, &unmarshaled)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expected != unmarshaled {
		t.Errorf("expected: %v, got: %v", expected, unmarshaled)
	}
}

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
		slices[i], err = key.Marshal()
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
