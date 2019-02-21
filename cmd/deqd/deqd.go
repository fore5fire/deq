package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore"
	eventserver "gitlab.com/katcheCode/deq/pkg/grpc/eventstore"
	"google.golang.org/grpc"
)

var (
	// debug indicates if debug mode is set
	debug = os.Getenv("DEBUG") == "true"

	// develop indicates if development mode is set
	develop = os.Getenv("DEVELOP") == "true"

	// listenAddress is the address that the grpc server will listen on
	listenAddress = os.Getenv("DEQ_LISTEN_ADDRESS")

	// dataDir is the database directory
	dataDir = os.Getenv("DEQ_DATA_DIR")

	// statsAddress is the address that deq publishes stats on
	statsAddress = os.Getenv("DEQ_STATS_ADDRESS")

	// deleteCorrupt specifies if we should allow the database to delete corrupt data.
	deleteCorrupt = strings.ToLower(os.Getenv("DEQ_DANGEROUS_DELETE_CORRUPT")) == "true"
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
	err := run(dataDir, listenAddress, statsAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("graceful shutdown complete")
}

func run(dbDir, address, statsAddress string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if statsAddress != "" {
		statsServer := http.Server{
			Addr:    statsAddress,
			Handler: http.DefaultServeMux,
		}
		defer func() {
			// Give the stats server 20 seconds to finish
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			err := statsServer.Shutdown(ctx)
			if err != nil {
				log.Printf("shutdown stats server gracefully: %v", err)
				statsServer.Close()
			}
		}()
		go func() {
			log.Printf("stats server listening on %s", statsAddress)
			err := statsServer.ListenAndServe()
			if err != http.ErrServerClosed {
				log.Printf("stats server listen: %v", err)
			}
		}()
	}

	err := os.MkdirAll(dbDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create data directory %s: %v", dbDir, err)
	}

	store, err := eventstore.Open(eventstore.Options{
		Dir:                    dbDir,
		DangerousDeleteCorrupt: deleteCorrupt,
	})
	if err != nil {
		return fmt.Errorf("open database: %v", err)
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
		return fmt.Errorf("bind %s: %v", address, err)
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
