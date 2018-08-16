package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

func main() {
	host := flag.String("host", "localhost:3000", "specify deq host and port. defaults to localhost:8080")
	flag.Parse()

	cmd := flag.Arg(0)
	switch cmd {
	case "list":
		conn, err := grpc.Dial(*host, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("dial host %s: %v", *host, err)
		}
		deqc := deq.NewDEQClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		stream, err := deqc.StreamEvents(ctx, &deq.StreamEventsRequest{
			Channel: strconv.FormatInt(int64(rand.Int()), 16),
		})
		if err != nil {
			log.Fatalf("stream: %v", err)
		}

		for {
			e, err := stream.Recv()
			if err != nil {
				log.Fatalf("recieve message: %v", err)
			}
			log.Printf("id: %v, type: %s", e.Id, e.Payload.TypeUrl)
		}

	case "help":
		flag.Usage()
	default:
		flag.Usage()
	}

}
