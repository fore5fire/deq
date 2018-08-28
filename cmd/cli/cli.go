package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	host := flag.String("host", "localhost:3000", "specify deq host and port. defaults to localhost:8080")
	channel := flag.String("c", strconv.FormatInt(int64(rand.Int()), 16), "specify channel. defaults to random")
	follow := flag.Bool("f", false, "continue streaming when idling. defaults to false")
	eventType := flag.String("t", "", "event type to print. defaults to all types")

	flag.Parse()

	if flag.NArg() != 1 {
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

		stream, err := deqc.StreamEvents(context.Background(), &deq.StreamEventsRequest{
			Channel: *channel,
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
			if *eventType == "" || strings.TrimPrefix(e.Payload.TypeUrl, "type.googleapis.com/") == *eventType {
				fmt.Printf("id: %v, type: %s\n", e.Id, e.Payload.TypeUrl)
			}
		}

	case "help":
		flag.Usage()
	default:
		flag.Usage()
	}

}
