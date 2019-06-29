package data

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalEventTimeKey(t *testing.T) {
	expected := EventTimeKey{
		Topic: "abc",
		ID:    "def",
	}
	buf, err := expected.Marshal(nil)
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
		t.Errorf("%s", cmp.Diff(expected, unmarshaled))
	}
}
