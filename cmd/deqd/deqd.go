package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore"
	eventserver "gitlab.com/katcheCode/deq/pkg/grpc/eventstore"
	"google.golang.org/grpc"
)

var (
	// Debug indicates if debug mode is set
	debug = os.Getenv("DEBUG") == "true"

	// Develop indicates if development mode is set
	develop = os.Getenv("DEVELOP") == "true"

	// ListenAddress is the address that the grpc server will listen on
	listenAddress = os.Getenv("DEQ_LISTEN_ADDRESS")

	// DataDir is the database directory
	dataDir = os.Getenv("DEQ_DATA_DIR")
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.LUTC)
	log.SetPrefix("")
}

func main() {
	log.Println("Starting up")

	if dataDir == "" {
		dataDir = "/var/deqd"
	}
	if listenAddress == "" {
		listenAddress = ":8080"
	}

	// run start code in seperate function so we can both defer and os.Exit
	err := run(dataDir, listenAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("graceful shutdown complete")
}

func run(dbDir, address string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := os.MkdirAll(dbDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Error creating data directory: %v", err)
	}

	store, err := eventstore.Open(eventstore.Options{
		Dir: dbDir,
	})
	if err != nil {
		return fmt.Errorf("Database directory %s could not be opened", dbDir)
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

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("Error binding %s", address)
	}

	log.Printf("gRPC server listening on %s", address)

	// Allow for graceful shutdown from SIGTERM or SIGINT
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGTERM)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		select {
		case <-ctx.Done():
			return
		case s := <-sig:
			log.Printf("recieved signal %v: shutting down...", s)
			grpcServer.Stop()
		}
	}()

	err = grpcServer.Serve(lis)
	if err != nil {
		return fmt.Errorf("gRPC server failed: %v", err)
	}

	return nil
}
