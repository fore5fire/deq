package deqdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
)

type EventKey struct {
	Topic, ID string
}

func TestBackupRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Setup database
	backupdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("make backup database directory: %v", err)
	}
	defer os.RemoveAll(backupdir)
	backupdb, err := Open(Options{
		Dir:   backupdir,
		Info:  &TestLogger{t, "RESTORE INFO"},
		Debug: &TestLogger{t, "RESTORE DEBUG"},
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		err := backupdb.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Write events to database
	expect := make(map[EventKey]deq.Event)
	e, err := backupdb.Pub(ctx, deq.Event{
		ID:      "testevent1",
		Topic:   "testtopic1",
		Payload: []byte("testpayload1"),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	expect[EventKey{e.Topic, e.ID}] = e
	e, err = backupdb.Pub(ctx, deq.Event{
		ID:      "testevent2",
		Topic:   "testtopic2",
		Payload: []byte("testpayload2"),
		Indexes: []string{"a", "c"},
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	expect[EventKey{e.Topic, e.ID}] = e

	// Run backup
	var bufs [3]CloseBuffer
	version, err := backupdb.Backup(&bufs[0], 0)
	if err != nil {
		t.Fatalf("run first backup: %v", err)
	}
	if version == 0 {
		t.Errorf("run first backup: verify version changed: no change")
	}

	// Write more events to database
	e, err = backupdb.Pub(ctx, deq.Event{
		ID:      "testevent3",
		Topic:   "testtopic2",
		Payload: []byte("testpayload3"),
		Indexes: []string{"b"},
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	expect[EventKey{e.Topic, e.ID}] = e

	// Backup new writes only
	newVersion, err := backupdb.Backup(&bufs[1], version)
	if err != nil {
		t.Fatalf("run incremental backup: %v", err)
	}
	if newVersion <= version {
		t.Errorf("run incremental backup: new version %d is not greater than previous %d", newVersion, version)
	}

	// Backup with no new data
	newVersion2, err := backupdb.Backup(&bufs[2], newVersion)
	if err != nil {
		t.Fatalf("run empty backup: %v", err)
	}
	if diff := cmp.Diff(newVersion, newVersion2); diff != "" {
		t.Errorf("run empty backup: verify version didn't change\n%s", diff)
	}
	if diff := cmp.Diff(0, bufs[2].Len()); diff != "" {
		t.Errorf("run empty backup: verify no bytes were written:\n%s", diff)
	}

	// Restore backup to a new database
	restoredir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("make backup database directory: %v", err)
	}
	defer os.RemoveAll(restoredir)
	restoredb, err := Open(Options{
		Dir:          restoredir,
		Info:         &TestLogger{t, "RESTORE INFO"},
		Debug:        &TestLogger{t, "RESTORE DEBUG"},
		BackupLoader: NewTestLoader(bufs[:]),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		err := restoredb.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Verify restored events
	err = restoredb.VerifyEvents(ctx, false)
	if err != nil {
		t.Errorf("verify restored events: %v", err)
	}

	// Verify restored events using get.
	for _, e := range expect {
		func() {
			channel := restoredb.Channel("testchannel", e.Topic)
			defer channel.Close()
			actual, err := channel.Get(ctx, e.ID)
			if err != nil {
				t.Errorf("get event %q: %v", e.ID, err)
				return
			}
			actual.Selector = e.Selector
			if diff := cmp.Diff(e, actual); diff != "" {
				t.Errorf("verify event:\n%s", diff)
			}
		}()
	}

	// Verify events by listing.
	topicChannel := restoredb.Channel("testchannel", deq.TopicsName)
	defer topicChannel.Close()
	topicIter := topicChannel.NewEventIter(nil)
	defer topicIter.Close()

	for topicIter.Next(ctx) {
		func() {
			topic := topicIter.Event().ID
			channel := restoredb.Channel("testchannel", topic)
			defer channel.Close()
			// Verify listing by IDs
			eventIter := channel.NewEventIter(nil)
			defer eventIter.Close()

			for eventIter.Next(ctx) {
				actual := eventIter.Event()
				e, ok := expect[EventKey{
					Topic: actual.Topic,
					ID:    actual.ID,
				}]
				if !ok {
					t.Errorf("list events by ID on topic %q: got unexpected event", actual)
					continue
				}
				actual.Selector = e.Selector
				if diff := cmp.Diff(e, actual); diff != "" {
					t.Errorf("list events by ID on topic %q: verify event:\n%s", topic, diff)
				}
			}
			if eventIter.Err() != nil {
				t.Errorf("list events by ID on topic %q: %v", topic, err)
			}

			// Verify listing by indexes
			indexIter := channel.NewEventIter(nil)
			defer indexIter.Close()

			for indexIter.Next(ctx) {
				actual := indexIter.Event()
				e, ok := expect[EventKey{
					Topic: actual.Topic,
					ID:    actual.ID,
				}]
				if !ok {
					t.Errorf("list events by index on topic %q: got unexpected event", actual)
					continue
				}
				actual.Selector = e.Selector
				if diff := cmp.Diff(e, actual); diff != "" {
					t.Errorf("list events by index on topic %q: verify event:\n%s", topic, diff)
				}
			}
			if indexIter.Err() != nil {
				t.Errorf("list events by index on topic %q: %v", topic, err)
			}

		}()
	}
	if topicIter.Err() != nil {
		t.Errorf("list topics: %v", topicIter.Err())
	}

}

type TestLoader struct {
	i    int
	bufs []CloseBuffer
}

func NewTestLoader(bufs []CloseBuffer) *TestLoader {
	return &TestLoader{
		bufs: bufs,
	}
}

func (l *TestLoader) NextBackup(ctx context.Context) (io.ReadCloser, error) {
	if l.i == len(l.bufs) {
		return nil, io.EOF
	}
	defer func() { l.i++ }()
	return &l.bufs[l.i], nil
}

type CloseBuffer struct {
	bytes.Buffer
	Closed bool
}

func (b *CloseBuffer) Close() error {
	if b.Closed {
		return errors.New("buffer already closed")
	}
	b.Closed = true
	return nil
}
