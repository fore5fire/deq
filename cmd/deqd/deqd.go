package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/auth"
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/handler"
	"gitlab.com/katcheCode/deq/deqdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// debug indicates if debug mode is set
	debug = strings.ToLower(os.Getenv("DEQ_DEBUG")) == "true"

	// listenAddress is the address that the grpc server will listen on
	listenAddress = os.Getenv("DEQ_LISTEN_ADDRESS")

	// dataDir is the database directory
	dataDir = os.Getenv("DEQ_DATA_DIR")

	// statsAddress is the address that deq publishes stats on
	statsAddress = os.Getenv("DEQ_STATS_ADDRESS")

	// KeepCorrupt prevents DEQ from deleting any corrupt data after an unclean shutdown. If true,
	// deqd will exit during startup of a database with corrupt data.
	keepCorrupt = strings.ToLower(os.Getenv("DEQ_KEEP_CORRUPT")) == "true"

	// requeueLimit specifies the default maximum requeues of a single event.
	requeueLimit = 40

	// listenInsecure sets the server to listen for HTTP2 requests with TLS disabled.
	insecure = strings.ToLower(os.Getenv("DEQ_LISTEN_INSECURE")) == "true"

	// tlsCertFile is the path of the tls certificate file. Required unless insecure is true.
	tlsCertFile = os.Getenv("DEQ_TLS_CERT_FILE")

	// tlsKeyFile is the path of the tls private key file. Required unless insecure is true.
	tlsKeyFile = os.Getenv("DEQ_TLS_KEY_FILE")

	// authRealm is the OAuth2 realm, the URL to which the client authentication challenge should be
	// made.
	authRealm = os.Getenv("DEQ_AUTH_REALM")

	// authService is the name of the service being authenticated. This must match the name used
	// to register this service with the auth provider. Required if authRealm is set.
	authService = os.Getenv("DEQ_AUTH_SERVICE")

	// authIssuer is the name of the issuer to allow in access tokens. Required if authRealm is set.
	authIssuer = os.Getenv("DEQ_AUTH_ISSUER")

	// authJWKSURI is the URI of a JSON Web Key Set used to retrieve token validation certificates.
	authJWKSURI = os.Getenv("DEQ_AUTH_JWKS_URI")

	// replicateTo is a comma-seperated list of deq hostnames that the server will replicate to.
	replicateTo = os.Getenv("DEQ_REPLICATE_TO")

	// replicateFrom is a comma-seperated list of deq hostnames that the server will replicate from.
	replicateFrom = os.Getenv("DEQ_REPLICATE_FROM")
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.LUTC)
	log.SetPrefix("")
}

func main() {
	log.Println("Starting up")

	if limit, ok := os.LookupEnv("DEQ_DEFAULT_REQUEUE_LIMIT"); ok {
		var err error
		requeueLimit, err = strconv.Atoi(limit)
		if err != nil {
			log.Fatalf("parse DEQ_REQUEUE_LIMIT from environment: %v", err)
		}
	}

	args := RunArgs{
		DataDir:     dataDir,
		ListenAddr:  listenAddress,
		StatsAddr:   statsAddress,
		AuthRealm:   authRealm,
		AuthIssuer:  authIssuer,
		AuthJWKSURI: authJWKSURI,
		TLSCertPath: tlsCertFile,
		TLSKeyPath:  tlsKeyFile,
	}

	if dataDir == "" {
		args.DataDir = "/var/deqd"
	}
	if args.ListenAddr == "" {
		args.ListenAddr = ":8080"
	}

	if args.AuthRealm != "" {
		if authService == "" {
			log.Fatal("auth service is required if auth realm is set")
		}
		if authIssuer == "" {
			log.Fatal("auth issuer is required if auth realm is set")
		}
	}

	// run start code in separate function so we can both defer and os.Exit
	err := run(args)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("graceful shutdown complete")
}

type RunArgs struct {
	// Required.
	DataDir string
	// Required.
	ListenAddr string
	StatsAddr  string
	Insecure   bool
	// Required unless Insecure is true
	TLSCertPath string
	// Required unless Insecure is true
	TLSKeyPath  string
	AuthRealm   string
	AuthIssuer  string
	AuthJWKSURI string
	ECDSAKeys   map[string]*ecdsa.PublicKey
	RSAKeys     map[string]*rsa.PublicKey
}

const authScopes = "events:read:all,events:write:all"

func run(args RunArgs) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if statsAddress != "" {
		statsServer := http.Server{
			Addr:    statsAddress,
			Handler: http.DefaultServeMux,
		}
		defer func() {
			// Give the stats server 5 seconds to finish
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			err := statsServer.Shutdown(ctx)
			if err != nil {
				log.Printf("shutdown stats server: %v", err)
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

	err := os.MkdirAll(args.DataDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create data directory %s: %v", args.DataDir, err)
	}

	opts := deqdb.Options{
		Dir:                 args.DataDir,
		KeepCorrupt:         keepCorrupt,
		DefaultRequeueLimit: requeueLimit,
		UpgradeIfNeeded:     true,
	}

	if debug {
		opts.Debug = log.New(os.Stdout, "DEBUG: ", log.Ltime|log.Lmicroseconds|log.LUTC)
	}

	store, err := deqdb.Open(opts)
	if err != nil {
		return fmt.Errorf("open database: %v", err)
	}
	defer store.Close()

	server := handler.New(store)

	var grpcopts []grpc.ServerOption

	// Setup grpc server for TLS
	if !insecure {
		creds, err := credentials.NewServerTLSFromFile(tlsCertFile, tlsKeyFile)
		if err != nil {
			return fmt.Errorf("load tls credentials: %v", err)
		}
		grpcopts = append(grpcopts, grpc.Creds(creds))
	}

	// Setup request authentication
	if args.AuthRealm != "" {
		grpcopts = append(
			grpcopts,
			grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

				// Get client-set metadata
				md, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					header := metadata.Pairs("WWW-Authenticate", fmt.Sprintf("realm=%q,service=%q,scopes=%q", args.AuthRealm, authService, authScopes))
					grpc.SendHeader(ctx, header)
					return nil, status.Error(codes.Unauthenticated, "")
				}

				// Verify Authorization header exists.
				authHeaders := md.Get("authorization")
				if len(authHeaders) == 0 {
					header := metadata.Pairs("WWW-Authenticate", fmt.Sprintf("realm=%q,service=%q,scopes=%q", args.AuthRealm, authService, authScopes))
					grpc.SendHeader(ctx, header)
					return nil, status.Error(codes.Unauthenticated, "")
				}

				// Reject if multiple authorization headers are set
				if len(authHeaders) > 1 {
					description := "multiple Authorization headers set"
					header := metadata.Pairs("WWW-Authenticate", fmt.Sprintf("realm=%q,service=%q,scopes=%q,error=%q,error_description=%q", args.AuthRealm, authService, authScopes, "invalid_header", description))
					grpc.SendHeader(ctx, header)
					return nil, status.Error(codes.Unauthenticated, description)
				}

				// Authorization header exists, verify it has a "bearer" token in a valid format.
				authorization := strings.SplitN(authHeaders[0], " ", 2)
				if len(authorization) < 2 || strings.ToLower(authorization[0]) != "bearer" {
					description := "Authorization header is not a bearer token"
					header := metadata.Pairs("WWW-Authenticate", fmt.Sprintf("realm=%q,service=%q,scopes=%q,error=%q,error_description=%q", args.AuthRealm, authService, authScopes, "invalid_header", description))
					grpc.SendHeader(ctx, header)
					return nil, status.Error(codes.Unauthenticated, description)
				}

				// Unmarshal and verify bearer token
				rawAccessToken := authorization[1]
				accessToken, err := auth.UnmarshalAndVerify(rawAccessToken)
				if err != nil {
					description := "bearer token is invalid"
					header := metadata.Pairs("WWW-Authenticate", fmt.Sprintf("realm=%q,service=%q,scopes=%q,error=%q,error_description=%q", authRealm, authService, authScopes, "invalid_token", description))
					grpc.SendHeader(ctx, header)
					return nil, status.Error(codes.Unauthenticated, description)
				}

				// Add access token to context and pass it to handler
				ctx = context.WithValue(ctx, auth.AccessTokenKey, accessToken)
				return handler(ctx, req)
			}),
			grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				return handler(srv, ss)
			}),
		)
	}

	grpcServer := grpc.NewServer(grpcopts...)

	pb.RegisterDEQServer(grpcServer, server)

	// Allow for graceful shutdown from SIGTERM or SIGINT
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGTERM)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		select {
		case <-ctx.Done():
			return
		case s := <-sig:
			log.Printf("received signal %v: shutting down...", s)
			grpcServer.Stop()
		}
	}()

	lis, err := net.Listen("tcp", args.ListenAddr)
	if err != nil {
		return fmt.Errorf("bind %s: %v", args.ListenAddr, err)
	}

	log.Printf("gRPC server listening on %s", args.ListenAddr)

	err = grpcServer.Serve(lis)
	if err != nil {
		return fmt.Errorf("gRPC server failed: %v", err)
	}

	return nil
}
