package handler

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/deqdb"
)

func TestList(t *testing.T) {
	ctx := context.Background()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal("create temp db dir: ", err)
	}
	db, err := deqdb.Open(deqdb.Options{
		Dir: dir,
	})
	if err != nil {
		t.Fatal("open db: ", err)
	}
	h := New(db)

	now := time.Now().UnixNano()

	expect := []*api.Event{
		{
			Id:            "1",
			Topic:         "test-topic",
			CreateTime:    now,
			DefaultState:  api.Event_QUEUED,
			State:         api.Event_QUEUED,
			SelectedIndex: -1,
		},
		{
			Id:            "2",
			Topic:         "test-topic",
			CreateTime:    now,
			DefaultState:  api.Event_QUEUED,
			State:         api.Event_QUEUED,
			SelectedIndex: -1,
		},
		{
			Id:            "3",
			Topic:         "test-topic",
			CreateTime:    now,
			DefaultState:  api.Event_QUEUED,
			State:         api.Event_QUEUED,
			SelectedIndex: -1,
		},
	}

	for _, e := range expect {
		_, err = h.Pub(ctx, &api.PubRequest{
			Event: e,
		})
		if err != nil {
			t.Fatal("pub: ", err)
		}
	}

	actual, err := h.List(ctx, &api.ListRequest{
		Topic:   "test-topic",
		Channel: "test-channel",
	})
	if err != nil {
		t.Fatal("list: ", err)
	}
	if !cmp.Equal(actual.Events, expect) {
		t.Error("\n", cmp.Diff(expect, actual.Events))
	}
}
