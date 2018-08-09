package main

import (
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

	err := os.MkdirAll(env.Dir, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating data directory: %v", err)
	}

	store, err := eventstore.Open(eventstore.Options{
		Dir: env.Dir,
	})
	if err != nil {
		log.Fatalf("Database directory %s could not be opened", env.Dir)
	}
	defer store.Close()

	server := eventserver.NewServer(store)

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDEQServer(grpcServer, server)

	lis, err := net.Listen("tcp", ":"+env.Port)
	if err != nil {
		log.Fatalf("Error binding port %s", env.Port)
	}

	log.Printf("gRPC server listening on port %s", env.Port)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}

	// handler.HandleFunc("/graphql", serveHTTP)
	//
	// server := &http.Server{
	// 	Addr:    ":" + env.Port,
	// 	Handler: handler,
	// }
	// log.Info().Str("port", env.Port).Msg("Starting server")
	// if err := server.ListenAndServe(); err != nil {
	// 	log.Fatal().Err(err).Msg("Startup failed")
	// }

}
