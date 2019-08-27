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
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/deqclient"
	"gitlab.com/katcheCode/deq/deqdb"
	"gitlab.com/katcheCode/deq/deqopt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
		fmt.Fprintln(w, "verify: Verify the database integrity. Only valid when a local database is opened using -dir.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Flags:")
		flag.PrintDefaults()
	}

	var host, channel, nameOverride, dir, min, max string
	var insecure, useIndex, all, debug, reversed, deleteInvalid bool
	var timeout, idle, workers int

	flag.StringVar(&host, "host", "localhost:3000", "Specify deq host and port. Ignored if dir flag is set.")
	flag.StringVar(&dir, "dir", "", "If set, use operate directly on the specified deq database directory instead of connecting to a deq server.")
	flag.StringVar(&channel, "channel", strconv.FormatInt(int64(rand.Int()), 16), "Specify channel.")
	flag.IntVar(&idle, "idle", 0, "The duration in milliseconds that the subscription waits if idle before closing. If 0, the subscription can idle indefinitely.")
	flag.IntVar(&timeout, "timeout", 0, "Timeout of the request in milliseconds. If 0 (the default), the request never times out.")
	flag.IntVar(&workers, "workers", 15, "The number of workers making concurrent requests. Must be positive.")
	flag.BoolVar(&insecure, "insecure", false, "Disables tls.")
	flag.StringVar(&nameOverride, "tls-name-override", "", "Overrides the expected name on the server's TLS certificate.")
	flag.BoolVar(&useIndex, "index", false, "Use the index instead of IDs for getting events.")
	flag.BoolVar(&all, "all", false, "Delete all events of the topic.")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging.")
	flag.StringVar(&min, "min", "", "Only list events with ID greater than or equal to min.")
	flag.StringVar(&max, "max", "", "Only list events with ID less than or equal to max.")
	flag.BoolVar(&reversed, "reverse", false, "List events from highest to lowest ID.")
	flag.BoolVar(&deleteInvalid, "delete-invalid", false, "Delete invalid events.")

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
	switch cmd {
	case "verify":
		if dir == "" {
			fmt.Fprintf(os.Stderr, "-dir is required for verify.")
			os.Exit(1)
		}

		opts := deqdb.Options{
			Dir:         dir,
			KeepCorrupt: true,
		}
		if debug {
			opts.Debug = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
		}
		db, err := deqdb.Open(opts)
		if err != nil {
			fmt.Printf("open database: %v\n", err)
			os.Exit(1)
		}

		err = db.VerifyEvents(ctx, deleteInvalid)
		if err != nil {
			fmt.Printf("verify events: %v\n", err)
		}

	case "sub":
		topic := flag.Arg(1)
		if topic == "" {
			fmt.Fprintf(os.Stderr, "topic is required")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v\n", err)
			os.Exit(1)
		}

		channel := deqc.Channel(channel, topic)
		defer channel.Close()

		channel.SetIdleTimeout(time.Duration(idle) * time.Millisecond)
		err = channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			fmt.Printf("ID: %v\nIndexes: %v\nPayload: %s\n\n", e.ID, e.Indexes, base64.StdEncoding.EncodeToString(e.Payload))
			return nil, ack.Error(ack.NoOp)
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
			os.Exit(2)
		}

	case "list", "topics":
		topic := flag.Arg(1)
		if cmd == "topics" {
			topic = "deq.events.Topic"
		}
		if topic == "" {
			fmt.Fprintf(os.Stderr, "topic is required")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v\n", err)
			os.Exit(1)
		}

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
			fmt.Fprintf(os.Stderr, "iterate: %v\n", iter.Err())
		}

	case "get":
		topic := flag.Arg(1)
		event := flag.Arg(2)
		if topic == "" {
			fmt.Fprintf(os.Stderr, "topic is required\n")
			os.Exit(1)
		}
		if event == "" {
			fmt.Fprintf(os.Stderr, "event is required\n")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v\n", err)
			os.Exit(1)
		}

		channel := deqc.Channel(channel, topic)
		defer channel.Close()

		var opts []deqopt.GetOption
		if useIndex {
			opts = append(opts, deqopt.UseIndex())
		}
		e, err := channel.Get(ctx, event, opts...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "get: %v\n", err)
			os.Exit(2)
		}

		printEvent(e)

	case "delete":
		topic := flag.Arg(1)
		if topic == "" {
			fmt.Fprintf(os.Stderr, "topic is required.\n")
			os.Exit(1)
		}

		id := flag.Arg(2)
		if id == "" && !all || id != "" && all {
			fmt.Fprintf(os.Stderr, "exactly one of argument <id> or flag -all is required.\n")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v\n", err)
			os.Exit(1)
		}

		if !all {
			// We're just deleting a single event.
			err = deqc.Del(ctx, topic, id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(2)
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
			func() {

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
					fmt.Fprintf(os.Stderr, "%v\n", iter.Err())
					os.Exit(2)
				}

			}()
		}

		close(events)
		wg.Wait()

		for _, topic := range topics {
			fmt.Printf("deleted %d events on topic %q\n", delCounts[topic], topic)
		}

	case "help", "":
		flag.CommandLine.SetOutput(os.Stdout)
		flag.Usage()
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command %s\n\n", cmd)
		flag.Usage()
	}

}

func open(dir, host, nameOverride string, insecure, debug bool) (deq.Client, error) {
	if dir != "" {
		opts := deqdb.Options{
			Dir:         dir,
			KeepCorrupt: true,
		}
		if debug {
			opts.Debug = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
		}
		db, err := deqdb.Open(opts)
		if err != nil {
			return nil, err
		}
		return deqdb.AsClient(db), nil
	}
	var opts []grpc.DialOption
	if insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		creds := credentials.NewTLS(&tls.Config{
			ServerName: nameOverride,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	// opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// 	log.Println(req)

	// 	err := invoker(ctx, method, req, reply, cc, opts...)
	// 	if err != nil {
	// 		status, ok := status.FromError(err)
	// 		if ok {

	// 		}
	// 		log.Println(err)
	// 		return err
	// 	}
	// 	log.Println(reply)
	// 	return nil
	// }))

	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial host %s: %v", host, err)
	}
	return deqclient.New(conn), nil
}

func printEvent(e deq.Event) {
	fmt.Printf("id: %v\n topic: %s\nstate: %v\nsend count: %d\nselector: %s\nselector version: %d\nindexes: %v\npayload: %s\n\n", e.ID, e.Topic, e.State, e.SendCount, e.Selector, e.SelectorVersion, e.Indexes, base64.StdEncoding.EncodeToString(e.Payload))
}
