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

	host := flag.String("host", "localhost:3000", "specify deq host and port.")
	channel := flag.String("c", strconv.FormatInt(int64(rand.Int()), 16), "specify channel.")
	follow := flag.Bool("f", false, "continue streaming when idling.")
	topic := flag.String("t", "", "topic to print. required.")

	flag.Parse()

	if flag.NArg() != 1 || *topic == "" {
		flag.Usage()
		os.Exit(1)
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "list":
		conn, err := grpc.Dial(*host, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("dial host %s: %v\n", *host, err)
			os.Exit(2)
		}
		deqc := deq.NewDEQClient(conn)

		stream, err := deqc.Sub(context.Background(), &deq.SubRequest{
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

	case "help":
		flag.Usage()
	default:
		flag.Usage()
	}

}
