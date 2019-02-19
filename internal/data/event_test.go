package data

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalEventKey(t *testing.T) {
	expected := EventKey{
		Topic: "abc",
		// Round to remove monotonic clock information (which won't get serilalized)
		CreateTime: time.Now(),
		ID:         "def",
	}
	buf, err := expected.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf[0] != EventTag {
		t.Errorf("expected serialized prefix %d, got %d", EventTag, buf[0])
	}

	expected.CreateTime = expected.CreateTime.Round(0)

	var unmarshaled EventKey
	err = UnmarshalTo(buf, &unmarshaled)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expected != unmarshaled {
		t.Errorf("expected: %v, got: %v", expected, unmarshaled)
	}

	expectedPrefix := buf[:len(expected.Topic)+3]
	prefix, err := EventTopicPrefix(expected.Topic)
	if err != nil {
		t.Fatalf("marshal prefix: %v", err)
	}
	if !bytes.Equal(expectedPrefix, prefix) {
		t.Errorf("marshal prefix:\n%s", cmp.Diff(expectedPrefix, prefix))
	}
}
