package main

import (
	"google.golang.org/grpc"
	"net"
	"os"
	pb "src.katchecode.com/deqd/api/v1/eventstore"
	"src.katchecode.com/deqd/env"
	"src.katchecode.com/deqd/eventstore"
	eventserver "src.katchecode.com/deqd/grpc/eventstore"
	"src.katchecode.com/deqd/logger"
)

var log = logger.Logger

func main() {
	log.Info().Msg("Starting up")

	err := os.MkdirAll(env.Dir, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating data directory")
	}

	store, err := eventstore.Open(eventstore.Options{
		Dir: env.Dir,
	})
	if err != nil {
		log.Fatal().Str("directory", env.Dir).Msg("Database could not be opened")
	}
	defer store.Close()

	server := eventserver.NewServer(store, env.ProtobufType)

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterEventStoreServer(grpcServer, server)

	lis, err := net.Listen("tcp", ":"+env.Port)
	if err != nil {
		log.Fatal().Str("port", env.Port).Msg("Error binding port")
	}

	log.Info().Str("port", env.Port).Msg("gRPC server listening")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("gRPC server failed")
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
