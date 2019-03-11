package eventdb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalIndexKey(t *testing.T) {
	expected := IndexKey{
		Topic: "abc",
		Value: "def",
		ID:    "abc",
	}
	buf, err := expected.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf[0] != IndexTag {
		t.Errorf("expected serialized prefix %d, got %d", IndexTag, buf[0])
	}

	var unmarshaled IndexKey
	err = UnmarshalTo(buf, &unmarshaled)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expected != unmarshaled {
		t.Errorf("%s", cmp.Diff(expected, unmarshaled))
	}
}
