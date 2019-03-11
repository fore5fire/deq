package eventdb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalChannelKey(t *testing.T) {
	expected := ChannelKey{
		Channel: "123",
		Topic:   "abc",
		ID:      "def",
	}
	buf, err := expected.Marshal(nil)
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
		t.Errorf("%s", cmp.Diff(expected, unmarshaled))
	}
}
