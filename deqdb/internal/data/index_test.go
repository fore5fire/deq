package data

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestMarshalIndexKey(t *testing.T) {
	t.Parallel()

	in := IndexKey{
		Topic:      "abc",
		Value:      "def",
		CreateTime: time.Unix(100, 0),
		ID:         "ghi",
	}

	tstr := string([]byte{0, 0, 0, 23, 72, 118, 232, 0})
	expect := []byte(string(IndexTag) + string(Sep) + "abc" + string(Sep) + "def" + string(Sep) + tstr + string(Sep) + "ghi")

	buf, err := in.Marshal(nil)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !cmp.Equal(expect, buf) {
		t.Errorf("marshal:\ngot:  %v\nwant: %v", expect, buf)
	}
}

func TestUnmarshalIndexKey(t *testing.T) {
	t.Parallel()

	expect := IndexKey{
		Topic:      "abc",
		Value:      "def",
		CreateTime: time.Unix(100, 0),
		ID:         "ghi",
	}

	tstr := string([]byte{0, 0, 0, 23, 72, 118, 232, 0})
	in := []byte(string(IndexTag) + string(Sep) + "abc" + string(Sep) + "def" + string(Sep) + tstr + string(Sep) + "ghi")

	var actual IndexKey
	err := UnmarshalTo(in, &actual)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if expect != actual {
		t.Errorf("unmarshal:\n%s", cmp.Diff(expect, actual))
	}
}

func TestIndexPrefixTopic(t *testing.T) {
	t.Parallel()

	topic := "abc"

	expectPrefix := []byte(string(IndexTag) + string(Sep) + topic + string(Sep))
	prefix, err := IndexPrefixTopic(topic)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !bytes.Equal(expectPrefix, prefix) {
		t.Errorf("\ngot:  %v\nwant: %v", expectPrefix, prefix)
	}
}
