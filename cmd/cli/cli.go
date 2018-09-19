package main

import (
	"context"
	"flag"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	host := flag.String("host", "localhost:3000", "specify deq host and port. defaults to localhost:8080")
	channel := flag.String("c", strconv.FormatInt(int64(rand.Int()), 16), "specify channel. defaults to random")
	follow := flag.Bool("f", false, "continue streaming when idling. defaults to false")
	topic := flag.String("t", "", "topic to print. required")

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
			log.Printf("id: %v, topic: %s", e.Id, e.Topic)
		}

	case "help":
		flag.Usage()
	default:
		flag.Usage()
	}

}
