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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"

	"gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Usage = func() {
		fmt.Println("Usage: deqctl [flags] COMMAND")
		fmt.Println("")
		fmt.Println("Available Commands:")
		fmt.Println("\t")
		fmt.Println("Available Flags:")
		fmt.Println("list: print events for a topic.")
		fmt.Println("topics: print all topics.")
		flag.PrintDefaults()
	}

	host := flag.String("host", "localhost:3000", "specify deq host and port.")
	channel := flag.String("c", strconv.FormatInt(int64(rand.Int()), 16), "specify channel.")
	follow := flag.Bool("f", false, "continue streaming when idling.")
	topic := flag.String("t", "", "topic to print. required.")
	timeout := flag.Int("timeout", 10000, "timeout of the request in milliseconds.")

	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Millisecond)
	defer cancel()

	cmd := flag.Arg(0)
	switch cmd {
	case "topics":
		conn, err := grpc.Dial(*host, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("dial host %s: %v\n", *host, err)
			os.Exit(2)
		}
		deqc := deq.NewDEQClient(conn)

		topics, err := deqc.Topics(ctx, &deq.TopicsRequest{})
		if err != nil {
			fmt.Printf("list topics: %v\n", err)
			os.Exit(2)
		}

		for _, topic := range topics.Topics {
			fmt.Println(topic)
		}
	case "list":
		if *topic == "" {
			flag.Usage()
			os.Exit(1)
		}

		conn, err := grpc.Dial(*host, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("dial host %s: %v\n", *host, err)
			os.Exit(2)
		}
		deqc := deq.NewDEQClient(conn)

		stream, err := deqc.Sub(ctx, &deq.SubRequest{
			Channel: *channel,
			Topic:   *topic,
			Follow:  *follow,
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
			fmt.Printf("id: %v, topic: %s, %s\n", e.Id, e.Topic, e.Payload)
		}

	case "help", "":
		flag.Usage()
	default:
		fmt.Printf("Error: unknown command %s\n\n", cmd)
		flag.Usage()
	}

}
