package main

import (
	"fmt"
	"log"
	"net"
	"os"

	pb "gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/env"
	"gitlab.com/katcheCode/deqd/pkg/eventstore"
	eventserver "gitlab.com/katcheCode/deqd/pkg/grpc/eventstore"
	"google.golang.org/grpc"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.LUTC)
	log.SetPrefix("")
}

func main() {
	log.Println("Starting up")
	// run start code in seperate function so we can both defer and os.Exit
	err := run()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Shutting down")
}

func run() error {

	err := os.MkdirAll(env.Dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Error creating data directory: %v", err)
	}

	store, err := eventstore.Open(eventstore.Options{
		Dir: env.Dir,
	})
	if err != nil {
		return fmt.Errorf("Database directory %s could not be opened", env.Dir)
	}
	defer store.Close()

	err = store.UpgradeDB()
	if err != nil {
		return fmt.Errorf("upgrade db: %v", err)
	}
	server := eventserver.NewServer(store)

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDEQServer(grpcServer, server)

	lis, err := net.Listen("tcp", ":"+env.Port)
	if err != nil {
		return fmt.Errorf("Error binding port %s", env.Port)
	}

	log.Printf("gRPC server listening on port %s", env.Port)

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %v", err)
	}

	return nil
}
