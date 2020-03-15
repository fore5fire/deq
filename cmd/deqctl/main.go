/*
Command deqctl provides a command line interface for DEQ servers.

to install deqctl, run:
	go install gitlab.com/katcheCode/deq/cmd/deqctl

for deqctl usage, run:
	deqctl help
*/
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	stdlog "log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/cmd/deqctl/internal/backup"
	_ "gitlab.com/katcheCode/deq/deqclient"
	"gitlab.com/katcheCode/deq/deqdb"
	"gitlab.com/katcheCode/deq/deqopt"
	"gitlab.com/katcheCode/deq/internal/log"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintln(w, "Usage: deqctl [flags] COMMAND")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Commands:")
		fmt.Fprintln(w, "list: Print events for a topic.")
		fmt.Fprintln(w, "sub: Print events for a topic as they are published.")
		fmt.Fprintln(w, "topics: Print all topics. Equivalent to list deq.events.Topic")
		fmt.Fprintln(w, "get: Get an event.")
		fmt.Fprintln(w, "delete: Delete an event.")
		fmt.Fprintln(w, "sync to: ")
		fmt.Fprintln(w, "sync from: ")
		fmt.Fprintln(w, "verify: Verify the database integrity. Only valid when a local database is opened using file:// scheme.")
		fmt.Fprintln(w, "backup save: save a backup to a directory or bucket. Only valid when a local database is opened using file:// scheme.")
		fmt.Fprintln(w, "backup load: load a backup from a directory or bucket. Only valid when a local database is opened using file:// scheme.")
		fmt.Fprintln(w, "keys: Print the individual keys in the database for debugging. Only valid when a local database is opened using file:// scheme.")
		// fmt.Fprintln(w, "repair topics: Attempt to rebuild the topics list by iterating events in the database. Only valid when a local database is opened using file:// scheme.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Flags:")
		flag.PrintDefaults()
	}

	var uri, channel, min, max, backupconn, topic, syncTarget string
	var insecure, useIndex, all, debug, reversed, deleteInvalid bool
	var timeout, idle, workers int

	flag.StringVar(&uri, "uri", "h2c://localhost:3000", "The deq connection string. Use scheme http or https to connect to a running deqd server, or file:// to open a local database without a server. When connecting with HTTPS, a san_override parameter can be added to the query string to override the expected certificate SAN.")
	flag.StringVar(&channel, "channel", strconv.FormatInt(int64(rand.Int()), 16), "Specify channel.")
	flag.IntVar(&idle, "idle", 0, "The duration in milliseconds that the subscription waits if idle before closing. If 0, the subscription can idle indefinitely.")
	flag.IntVar(&timeout, "timeout", 0, "Timeout of the request in milliseconds. If 0 (the default), the request never times out.")
	flag.IntVar(&workers, "workers", 15, "The number of workers making concurrent requests. Must be positive.")
	flag.BoolVar(&insecure, "insecure", false, "Disables tls.")
	flag.BoolVar(&useIndex, "index", false, "Use the index instead of IDs for getting events.")
	flag.BoolVar(&all, "all", false, "Sync or delete all events of the topic.")
	flag.StringVar(&min, "min", "", "Only list events with ID greater than or equal to min.")
	flag.StringVar(&max, "max", "", "Only list events with ID less than or equal to max.")
	flag.BoolVar(&reversed, "reverse", false, "List events from highest to lowest ID.")
	flag.BoolVar(&deleteInvalid, "delete-invalid", false, "Delete invalid events.")
	flag.StringVar(&backupconn, "backupconn", "", "Backup directory or bucket connection string, parsed by the Go CDK blob package.")
	flag.StringVar(&topic, "topic", "", "The topic to sync.")
	flag.StringVar(&syncTarget, "target", "", "The target to sync to.")

	flag.Parse()

	var cancel func()
	var ctx context.Context
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	}
	defer cancel()

	cmd := flag.Arg(0)

	err := func() error {
		switch cmd {
		case "verify":
			if uri == "" {
				return fmt.Errorf("-uri is required")
			}

			opts, err := deqdb.ParseConnectionString(uri)
			if err != nil {
				return fmt.Errorf("parse connection string: %v", err)
			}
			db, err := deqdb.Open(opts)
			if err != nil {
				return fmt.Errorf("open database: %v", err)
			}

			err = db.VerifyEvents(ctx, deleteInvalid)
			if err != nil {
				return fmt.Errorf("verify events: %v", err)
			}

		case "sub":
			topic := flag.Arg(1)
			if topic == "" {
				return fmt.Errorf("topic is required")
			}

			deqc, err := deq.Open(ctx, uri)
			if err != nil {
				return fmt.Errorf("open client: %v", err)
			}
			defer func() {
				err := deqc.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close client: %v\n", err)
				}
			}()

			channel := deqc.Channel(channel, topic)
			defer channel.Close()

			channel.SetIdleTimeout(time.Duration(idle) * time.Millisecond)
			err = channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
				fmt.Printf("ID: %v\nIndexes: %v\nPayload: %s\n\n", e.ID, e.Indexes, base64.StdEncoding.EncodeToString(e.Payload))
				return nil, ack.Error(ack.NoOp)
			})
			if err != nil {
				return fmt.Errorf("subscribe: %v", err)
			}
			return nil

		case "list", "topics":
			topic := flag.Arg(1)
			if cmd == "topics" {
				topic = "deq.events.Topic"
			}
			if topic == "" {
				return fmt.Errorf("topic is required")
			}

			deqc, err := deq.Open(ctx, uri)
			if err != nil {
				return fmt.Errorf("open client: %v", err)
			}
			defer func() {
				err := deqc.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close client: %v\n", err)
				}
			}()

			channel := deqc.Channel(channel, topic)
			defer channel.Close()

			opts := &deq.IterOptions{
				Min:      min,
				Max:      max,
				Reversed: reversed,
			}
			var iter deq.EventIter
			if useIndex {
				iter = channel.NewIndexIter(opts)
			} else {
				iter = channel.NewEventIter(opts)
			}
			defer iter.Close()

			for ctx.Err() == nil {
				for iter.Next(ctx) {
					e := iter.Event()
					printEvent(e)
				}
				if iter.Err() == nil {
					break
				}
				fmt.Fprintf(os.Stderr, "iterate: %v", iter.Err())
			}

		case "get":
			topic := flag.Arg(1)
			event := flag.Arg(2)
			if topic == "" {
				return fmt.Errorf("topic is required")
			}
			if event == "" {
				return fmt.Errorf("event is required")
			}

			deqc, err := deq.Open(ctx, uri)
			if err != nil {
				return fmt.Errorf("open client: %v", err)
			}
			defer func() {
				err := deqc.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close client: %v\n", err)
				}
			}()

			channel := deqc.Channel(channel, topic)
			defer channel.Close()

			var opts []deqopt.GetOption
			if useIndex {
				opts = append(opts, deqopt.UseIndex())
			}
			e, err := channel.Get(ctx, event, opts...)
			if err != nil {
				return fmt.Errorf("get: %v", err)
			}

			printEvent(e)

		case "delete":
			topic := flag.Arg(1)
			if topic == "" {
				return fmt.Errorf("topic is required")
			}

			id := flag.Arg(2)
			if id == "" && !all || id != "" && all {
				return fmt.Errorf("exactly one of argument <id> or flag -all is required")
			}

			deqc, err := deq.Open(ctx, uri)
			if err != nil {
				return fmt.Errorf("open client: %v", err)
			}
			defer func() {
				err := deqc.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close client: %v\n", err)
				}
			}()

			if !all {
				// We're just deleting a single event.
				err = deqc.Del(ctx, topic, id)
				if err != nil {
					return fmt.Errorf("%v", err)
				}
				break
			}

			events := make(chan deq.Event, workers)
			var mut sync.Mutex
			var wg sync.WaitGroup
			delCounts := make(map[string]int)

			wg.Add(workers)
			for i := 0; i < workers; i++ {
				go func() {
					defer wg.Done()
					for e := range events {
						// for retries := 0; retries < 10; retries++ {
						err = deqc.Del(ctx, e.Topic, e.ID)
						// if status.Code(err) == codes.Internal {
						// 	continue
						// }
						if err != nil {
							fmt.Printf("delete event %q %q: %v\n", e.Topic, e.ID, err)
							continue
						}
						mut.Lock()
						delCounts[e.Topic]++
						fmt.Printf("deleted %q %q\n", e.Topic, e.ID)
						mut.Unlock()
					}
					// mut.Lock()
					// fmt.Printf("%q %q exceeded retries\n", topic, id)
					// mut.Unlock()
					// }
				}()
			}

			topics := strings.Split(topic, ",")
			for _, topic := range topics {
				err := func() error {

					// We're deleting all events on the topic. Loop through each event and delete it.
					channel := deqc.Channel(channel, topic)
					defer channel.Close()

					iter := channel.NewEventIter(&deq.IterOptions{
						Min:           min,
						Max:           max,
						PrefetchCount: workers * 3,
					})
					defer iter.Close()
					for iter.Next(ctx) {
						events <- iter.Event()
					}
					if iter.Err() != nil {
						return fmt.Errorf("%v", iter.Err())
					}

					return nil
				}()
				if err != nil {
					return err
				}
			}

			close(events)
			wg.Wait()

			for _, topic := range topics {
				fmt.Printf("deleted %d events on topic %q\n", delCounts[topic], topic)
			}

		case "backup":
			switch flag.Arg(1) {
			case "save":
				return fmt.Errorf("not implemented")
				// opts := deqdb.Options{
				// 	Dir:         dir,
				// 	KeepCorrupt: true,
				// }
				// if debug {
				// 	opts.Debug = stdlog.New(os.Stdout, "DEBUG: ", stdlog.LstdFlags)
				// }
				// db, err := deqdb.Open(opts)
				// if err != nil {
				// 	fmt.Printf("open database: %v\n", err)
				// 	os.Exit(1)
				// }

				// db.Backup()

			case "load":
				if backupconn == "" {
					fmt.Fprintf(os.Stderr, "-backupconn is required\n")
				}
				if uri == "" {
					fmt.Fprintf(os.Stderr, "-uri is required\n")
				}
				var logger log.Logger
				if debug {
					logger = stdlog.New(os.Stdout, "LOADER DEBUG: ", stdlog.LstdFlags)
				}

				loader, err := backup.NewLoader(ctx, backupconn, logger)
				if err != nil {
					return fmt.Errorf("create backup loader: %v", err)
				}

				opts, err := deqdb.ParseConnectionString(uri)
				if err != nil {
					return fmt.Errorf("parse connection string: %v", err)
				}
				opts.BackupLoader = loader
				db, err := deqdb.Open(opts)
				if err != nil {
					return fmt.Errorf("open db: %v", err)
				}
				err = db.Close()
				if err != nil {
					return fmt.Errorf("close database: %v", err)
				}

			default:
				fmt.Fprintf(os.Stderr, "Error: unknown command backup %s\n\n", flag.Arg(1))
				flag.Usage()
			}

		case "keys":
			if uri == "" {
				fmt.Fprintf(os.Stderr, "-uri is required\n")
			}
			deqOptions, err := deqdb.ParseConnectionString(uri)
			if err != nil {
				return fmt.Errorf("%v", err)
			}

			logger := &log.BadgerLogger{
				Error: stdlog.New(os.Stderr, "", stdlog.LstdFlags),
				Warn:  stdlog.New(os.Stderr, "WARN: ", stdlog.LstdFlags),
				Info:  deqOptions.Info,
				Debug: deqOptions.Debug,
			}
			opts := badger.
				DefaultOptions(deqOptions.Dir).
				WithReadOnly(true).
				WithLogger(logger)

			db, err := badger.Open(opts)
			if err != nil {
				return fmt.Errorf("open badger db: %v", err)
			}
			defer func() {
				err := db.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close badger db: %v\n", err)
				}
			}()

			txn := db.NewTransaction(false)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			var count uint64
			for it.Rewind(); it.Valid(); it.Next() {
				count++
				item := it.Item()
				err := item.Value(func(val []byte) error {
					fmt.Printf("key: %q\nval: %q\nversion: %x\n\n", item.Key(), val, item.Version())
					return nil
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "get value for key %q: %v - skipping\n", item.Key(), err)
				}
			}

			fmt.Printf("got %d items\n", count)

		case "sync":
			if syncTarget == "" {
				return fmt.Errorf("-target is required")
			}

			source, err := deq.Open(ctx, uri)
			if err != nil {
				return fmt.Errorf("open source client: %v", err)
			}
			defer func() {
				err := source.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close source client: %v\n", err)
				}
			}()

			target, err := deq.Open(ctx, syncTarget)
			if err != nil {
				fmt.Fprintf(os.Stderr, "open target client: %v\n", err)
			}
			defer func() {
				err := target.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "close target client: %v\n", err)
				}
			}()

			var logger log.Logger = log.NoOpLogger{}
			if debug {
				logger = stdlog.New(os.Stdout, "DEBUG: ", stdlog.Ltime)
			}

			switch {
			case topic != "":
				okCount, errCount := syncTopic(ctx, source, target, topic, channel, logger)
				fmt.Printf("%d events ok, %d events failed\n", okCount, errCount)

			case all:
				c := source.Channel(channel, "deq.events.Topic")
				defer c.Close()

				it := c.NewEventIter(nil)
				defer it.Close()

				var okEvents, errEvents, okTopics, errTopics int
				for {
					for it.Next(ctx) {
						topic := it.Event()
						logger.Printf("starting topic %q...", topic.ID)
						okTopics++

						okCount, errCount := syncTopic(ctx, source, target, topic.ID, channel, logger)
						fmt.Printf("finished topic %q: %d events ok, %d events failed\n", topic.ID, okCount, errCount)
						okEvents += okCount
						errEvents += errCount
					}
					if it.Err() == nil {
						break
					}
					fmt.Fprintf(os.Stderr, "list topic: %v", it.Err())
					errTopics++
				}

				fmt.Printf("totals: %d topics ok, %d events ok, %d topics failed, %d events failed", okTopics, okEvents, errTopics, errEvents)

			default:
				return fmt.Errorf("Either -topic or -all is required")
			}

		// case "repair":
		// 	switch flag.Arg(1) {
		// 	case "topics":

		// 	default:
		// 		fmt.Fprintf(os.Stderr, "Error: unknown command repair %s\n\n", flag.Arg(1))
		// 		flag.Usage()
		// 	}

		case "help", "":
			flag.CommandLine.SetOutput(os.Stdout)
			flag.Usage()
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown command %s\n\n", cmd)
			flag.Usage()
		}

		return nil
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func syncTopic(ctx context.Context, source, target deq.Client, topic, channel string, debug log.Logger) (okEvents int, errEvents int) {
	c := source.Channel(channel, topic)
	defer c.Close()

	it := c.NewEventIter(nil)
	defer it.Close()

	for {
		for it.Next(ctx) {
			e := it.Event()
			_, err := target.Pub(ctx, e)
			if err != nil {
				errEvents++
				fmt.Fprintf(os.Stderr, "publish event %q: %v", e.ID, err)
				continue
			}
			debug.Printf("synced event %q: %v", e.ID, err)
			okEvents++
		}
		if it.Err() == nil {
			return okEvents, errEvents
		}
		fmt.Fprintf(os.Stderr, "list events: %v\n", it.Err())
		errEvents++
	}
}

func printEvent(e deq.Event) {
	fmt.Printf("id: %v\n topic: %s\ncreate time: %v\nstate: %v\nsend count: %d\nselector: %s\nselector version: %d\nindexes: %v\npayload: %s\n\n", e.ID, e.Topic, e.CreateTime, e.State, e.SendCount, e.Selector, e.SelectorVersion, e.Indexes, base64.StdEncoding.EncodeToString(e.Payload))
}
