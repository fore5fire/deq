package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
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
			log.Fatalf("dial host %s: %v", *host, err)
		}
		deqc := deq.NewDEQClient(conn)

		stream, err := deqc.Sub(context.Background(), &deq.SubRequest{
			Channel: *channel,
			Topic:   *topic,
			Follow:  *follow,
		})
		if err != nil {
			log.Fatalf("stream: %v", err)
		}

		for {
			e, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("recieve message: %v", err)
			}
			fmt.Printf("id: %v, topic: %s, %s\n", e.Id, e.Topic, e.Payload)
		}

	case "help":
		flag.Usage()
	default:
		flag.Usage()
	}

}
