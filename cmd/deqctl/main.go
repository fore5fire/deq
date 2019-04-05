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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc/credentials"

	"gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Usage = func() {
		fmt.Println("Usage: deqctl [flags] COMMAND")
		fmt.Println("")
		fmt.Println("Available Commands:")
		fmt.Println("list: print events for a topic.")
		fmt.Println("topics: print all topics.")
		fmt.Println("")
		fmt.Println("Available Flags:")
		flag.PrintDefaults()
	}

	var host, channel, topic, nameOverride string
	var follow, insecure bool
	var timeout int

	flag.StringVar(&host, "host", "localhost:3000", "specify deq host and port.")
	flag.StringVar(&channel, "c", strconv.FormatInt(int64(rand.Int()), 16), "specify channel.")
	flag.BoolVar(&follow, "f", false, "continue streaming when idling.")
	flag.StringVar(&topic, "t", "", "topic to print. required.")
	flag.IntVar(&timeout, "timeout", 10000, "timeout of the request in milliseconds.")
	flag.BoolVar(&insecure, "insecure", false, "disables tls")
	flag.StringVar(&nameOverride, "tls-name-override", "", "overrides the expected name on the server's TLS certificate.")

	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	cmd := flag.Arg(0)
	switch cmd {
	case "topics":
		deqc, err := dial(host, nameOverride, insecure)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
			os.Exit(1)
		}

		topics, err := deqc.Topics(ctx, &deq.TopicsRequest{})
		if err != nil {
			fmt.Printf("list topics: %v\n", err)
			os.Exit(2)
		}

		for _, topic := range topics.Topics {
			fmt.Println(topic)
		}
	case "list":
		if topic == "" {
			fmt.Printf("flag -topic is required")
			os.Exit(1)
		}

		deqc, err := dial(host, nameOverride, insecure)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
			os.Exit(1)
		}

		stream, err := deqc.Sub(ctx, &deq.SubRequest{
			Channel: channel,
			Topic:   topic,
			Follow:  follow,
		})
		if err != nil {
			fmt.Printf("stream: %v\n", err)
			os.Exit(2)
		}

		for {
			e, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("recieve message: %v\n", err)
				os.Exit(2)
			}
			fmt.Printf("id: %v, topic: %s\nindexes: %v\n %s\n\n", e.Id, e.Topic, e.Indexes, e.Payload)
		}

	case "delete":
		if topic == "" {
			fmt.Printf("flag -topic is required.\n")
			os.Exit(1)
		}
		id := flag.Arg(1)
		if id == "" {
			fmt.Printf("argument <id> is required.\n")
			os.Exit(1)
		}

		deqc, err := dial(host, nameOverride, insecure)
		if err != nil {
			fmt.Printf("dial: %v\n", err)
			os.Exit(1)
		}

		_, err = deqc.Del(ctx, &deq.DelRequest{
			Topic:   topic,
			EventId: id,
		})
		if err != nil {
			fmt.Printf("%v", err)
			os.Exit(2)
		}
	case "help", "":
		flag.Usage()
	default:
		fmt.Printf("Error: unknown command %s\n\n", cmd)
		flag.Usage()
	}

}

func dial(host, nameOverride string, insecure bool) (deq.DEQClient, error) {
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
	return deq.NewDEQClient(conn), nil
}
