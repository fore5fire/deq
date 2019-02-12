package data

import "testing"

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
