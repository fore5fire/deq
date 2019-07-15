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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Usage = func() {
		fmt.Println("Usage: deqctl [flags] COMMAND")
		fmt.Println("")
		fmt.Println("Available Commands:")
		fmt.Println("list: print events for a topic.")
		fmt.Println("sub: print events for a topic as they are published.")
		fmt.Println("topics: print all topics. Equivalent to list deq.events.Topic")
		fmt.Println("get: get an event.")
		fmt.Println("delete: delete an event.")
		fmt.Println("")
		fmt.Println("Available Flags:")
		flag.PrintDefaults()
	}

	var host, channel, nameOverride, dir, min, max string
	var insecure, useIndex, all, debug, reversed bool
	var timeout, idle, workers int

	flag.StringVar(&host, "host", "localhost:3000", "Specify deq host and port. Ignored if dir flag is set.")
	flag.StringVar(&dir, "dir", "", "If set, use operate directly on the specified deq database directory instead of connecting to a deq server.")
	flag.StringVar(&channel, "channel", strconv.FormatInt(int64(rand.Int()), 16), "Specify channel.")
	flag.IntVar(&idle, "idle", 0, "The duration in milliseconds that the subscription waits if idle before closing. If 0, the subscription can idle indefinitely.")
	flag.IntVar(&timeout, "timeout", 10000, "Timeout of the request in milliseconds. If 0, the request never times out.")
	flag.IntVar(&workers, "workers", 15, "The number of workers making concurrent requests. Must be positive.")
	flag.BoolVar(&insecure, "insecure", false, "Disables tls")
	flag.StringVar(&nameOverride, "tls-name-override", "", "Overrides the expected name on the server's TLS certificate.")
	flag.BoolVar(&useIndex, "index", false, "Use the index instead of IDs for getting events")
	flag.BoolVar(&all, "all", false, "Delete all events of the topic")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging.")
	flag.StringVar(&min, "min", "", "Only list events with ID greater than or equal to min.")
	flag.StringVar(&max, "max", "", "Only list events with ID less than or equal to max.")
	flag.BoolVar(&reversed, "reverse", false, "List events from highest to lowest ID.")

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
	case "sub":
		topic := flag.Arg(1)
		if topic == "" {
			fmt.Printf("topic is required")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
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
			fmt.Printf("subscribe: %v\n", err)
			os.Exit(2)
		}

	case "list", "topics":
		topic := flag.Arg(1)
		if cmd == "topics" {
			topic = "deq.events.Topic"
		}
		if topic == "" {
			fmt.Printf("topic is required")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
			os.Exit(1)
		}

		channel := deqc.Channel(channel, topic)
		defer channel.Close()

		iter := channel.NewEventIter(&deq.IterOptions{
			Min:      min,
			Max:      max,
			Reversed: reversed,
		})
		defer iter.Close()

		for ctx.Err() == nil {
			for iter.Next(ctx) {
				e := iter.Event()
				fmt.Printf("ID: %v\nIndexes: %v\nPayload: %s\n\n", e.ID, e.Indexes, base64.StdEncoding.EncodeToString(e.Payload))
			}
			if iter.Err() == nil {
				break
			}
			fmt.Printf("iterate: %v\n", iter.Err())
		}

	case "get":
		topic := flag.Arg(1)
		event := flag.Arg(2)
		if topic == "" {
			fmt.Printf("topic is required\n")
			os.Exit(1)
		}
		if event == "" {
			fmt.Printf("event is required\n")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
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
			fmt.Printf("get: %v\n", err)
			os.Exit(2)
		}

		fmt.Printf("id: %v, topic: %s\nindexes: %v\n %s\n\n", e.ID, e.Topic, e.Indexes, e.Payload)

	case "delete":
		topic := flag.Arg(1)
		if topic == "" {
			fmt.Printf("topic is required.\n")
			os.Exit(1)
		}

		id := flag.Arg(2)
		if id == "" && !all || id != "" && all {
			fmt.Printf("exactly one of argument <id> or flag -all is required.\n")
			os.Exit(1)
		}

		deqc, err := open(dir, host, nameOverride, insecure, debug)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
			os.Exit(1)
		}

		if !all {
			// We're just deleting a single event.
			err = deqc.Del(ctx, topic, id)
			if err != nil {
				fmt.Printf("%v", err)
				os.Exit(2)
			}
			break
		}

		topics := strings.Split(topic, ",")
		for _, topic := range topics {
			func() {

				// We're deleting all events on the topic. Loop through each event and delete it.
				delCount := 0
				channel := deqc.Channel(channel, topic)
				defer channel.Close()

				ids := make(chan string, workers)
				var mut sync.Mutex
				var wg sync.WaitGroup

				wg.Add(workers)
				for i := 0; i < workers; i++ {
					go func() {
						defer wg.Done()
						for id := range ids {
							for retries := 0; retries < 10; retries++ {
								err = deqc.Del(ctx, topic, id)
								if status.Code(err) == codes.Internal {
									continue
								}
								if err != nil {
									fmt.Printf("delete event %q %q: %v", topic, id, err)
									break
								}
								mut.Lock()
								delCount++
								fmt.Printf("deleted %q %q\n", topic, id)
								mut.Unlock()
								break
							}
							mut.Lock()
							fmt.Printf("%q %q exceeded retries\n", topic, id)
							mut.Unlock()
						}
					}()
				}

				iter := channel.NewEventIter(nil)
				defer iter.Close()
				for iter.Next(ctx) {
					ids <- iter.Event().ID
				}
				if iter.Err() != nil {
					fmt.Printf("%v", iter.Err())
					os.Exit(2)
				}
				close(ids)

				wg.Wait()

				fmt.Printf("deleted %d events on topic %q\n", delCount, topic)
			}()
		}

	case "help", "":
		flag.Usage()
	default:
		fmt.Printf("Error: unknown command %s\n\n", cmd)
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
