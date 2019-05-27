package greeter

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb"
)

func TestHelloReplyIter(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal("create db dir: ", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()

	db, err := deqdb.Open(deqdb.Options{
		Dir: dir,
	})
	if err != nil {
		t.Fatal("open db: ", err)
	}

	client := NewGreeterClient(deqdb.AsClient(db), "test-channel", nil)

	now := time.Now().Round(0)

	expect := []*HelloReplyEvent{
		{
			ID:           "1",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"9", "8"},
			HelloReply: &HelloReply{
				Message: "Hello world!",
			},
		},
		{
			ID:           "2",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"1"},
			HelloReply: &HelloReply{
				Message: "Hello again world!",
			},
		},
	}

	for i, e := range expect {
		_, err := client.PubHelloReplyEvent(ctx, e)
		if err != nil {
			t.Fatalf("pub event %d: %v", i, err)
		}
	}

	iter := client.NewHelloReplyEventIter(nil)

	var actual []*HelloReplyEvent
	for {
		e, err := iter.Next(ctx)
		if err == deq.ErrIterationComplete {
			break
		}
		if err != nil {
			t.Fatal("next: ", err)
		}
		actual = append(actual, e)
	}
	if !cmp.Equal(expect, actual) {
		t.Error("\n", cmp.Diff(expect, actual))
	}

	iter = client.NewHelloReplyIndexIter(nil)

	expect = []*HelloReplyEvent{
		{
			ID:           "2",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"1"},
			HelloReply: &HelloReply{
				Message: "Hello again world!",
			},
		},
		{
			ID:           "1",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"9", "8"},
			HelloReply: &HelloReply{
				Message: "Hello world!",
			},
		},
		{
			ID:           "1",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"9", "8"},
			HelloReply: &HelloReply{
				Message: "Hello world!",
			},
		},
	}

	actual = actual[:0]
	for {
		e, err := iter.Next(ctx)
		if err == deq.ErrIterationComplete {
			break
		}
		if err != nil {
			t.Fatal("next: ", err)
		}
		actual = append(actual, e)
	}
	if !cmp.Equal(expect, actual) {
		t.Error("\n", cmp.Diff(expect, actual))
	}
}
