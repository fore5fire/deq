package data

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalIndexKey(t *testing.T) {
	expected := IndexKey{
		Topic: "abc",
		Value: "def",
	}
	expectedBuf := []byte{IndexTag, Sep, 'a', 'b', 'c', Sep, 'd', 'e', 'f'}

	buf, err := expected.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Equal(buf, expectedBuf) {
		t.Errorf("serialize:\n%s", cmp.Diff(expectedBuf, buf))
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
