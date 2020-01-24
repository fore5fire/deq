package main

import (
	"context"
	"fmt"
	stdlog "log"
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
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/backup"
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/handler"
	"gitlab.com/katcheCode/deq/deqdb"
	"gitlab.com/katcheCode/deq/internal/log"
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

	// certFile is the path of the tls certificate file. Required unless insecure is true.
	certFile = os.Getenv("DEQ_TLS_CERT_FILE")

	// keyFile is the path of the tls private key file. Required unless insecure is true.
	keyFile = os.Getenv("DEQ_TLS_KEY_FILE")

	authSharedSecret = os.Getenv("DEQ_AUTH_SHARED_SECRET")

	// backupMode sets the deqd backup behavior. Valid options are "load_only",
	// "save_only", "sync", or "disabled". Defaults to "disabled".
	backupMode = os.Getenv("DEQ_BACKUP_MODE")

	// backupIntervalSeconds is the number of seconds to wait between writing
	// backups if enabled. Backups are only read on startup if backup reading is
	// enabled. Defaults to 5 minutes.
	backupIntervalSeconds = os.Getenv("DEQ_BACKUP_INTERVAL_SECONDS")

	// backupURI is the cloud storage connection string or filesystem directory to
	// write database backups.
	backupURI = os.Getenv("DEQ_BACKUP_URI")
)

var backupInterval = time.Second * 300

func init() {
	stdlog.SetFlags(stdlog.Ldate | stdlog.Ltime | stdlog.Lshortfile | stdlog.LUTC)
	stdlog.SetPrefix("")
}

var (
	BackupModeDisabled = "disabled"
	BackupModeLoadOnly = "load_only"
	BackupModeSaveOnly = "save_only"
	BackupModeSync     = "sync"
)

func main() {
	stdlog.Println("Starting up")

	if limit, ok := os.LookupEnv("DEQ_DEFAULT_REQUEUE_LIMIT"); ok {
		var err error
		requeueLimit, err = strconv.Atoi(limit)
		if err != nil {
			stdlog.Fatalf("parse DEQ_DEFAULT_REQUEUE_LIMIT from environment: %v", err)
		}
	}

	if backupIntervalSeconds != "" {
		seconds, err := strconv.ParseInt(backupIntervalSeconds, 10, 32)
		if err != nil {
			stdlog.Fatalf("parse DEQ_BACKUP_INTERVAL_SECONDS: %v", err)
		}
		backupInterval = time.Duration(seconds) * time.Second
	}

	if backupMode == "" {
		backupMode = BackupModeDisabled
	}
	backupMode = strings.ToLower(backupMode)
	if backupMode != BackupModeDisabled && backupMode != BackupModeSaveOnly &&
		backupMode != BackupModeLoadOnly && backupMode != BackupModeSync {
		stdlog.Fatalf("parse DEQ_BACKUP_MODE: invalid option, choose one of %q %q %q %q", BackupModeDisabled, BackupModeLoadOnly, BackupModeSaveOnly, BackupModeSync)
	}

	if dataDir == "" {
		dataDir = "/var/deqd"
	}
	if listenAddress == "" {
		listenAddress = ":8080"
	}

	// run start code in separate function so we can both defer and os.Exit
	err := run(dataDir, listenAddress, statsAddress, certFile, keyFile, insecure)
	if err != nil {
		stdlog.Fatal(err)
	}

	stdlog.Println("graceful shutdown complete")
}

func run(dbDir, address, statsAddress, certFile, keyFile string, insecure bool) error {
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
				stdlog.Printf("shutdown stats server gracefully: %v", err)
				statsServer.Close()
			}
		}()
		go func() {
			stdlog.Printf("stats server listening on %s", statsAddress)
			err := statsServer.ListenAndServe()
			if err != http.ErrServerClosed {
				stdlog.Printf("stats server listen: %v", err)
			}
		}()
	}

	var b *backup.Backup
	var loader deqdb.Loader
	loadBackups := backupMode == BackupModeLoadOnly || backupMode == BackupModeSync
	saveBackups := backupMode == BackupModeSaveOnly || backupMode == BackupModeSync
	if loadBackups || saveBackups {
		var logger log.Logger
		var err error
		if debug {
			logger = stdlog.New(os.Stdout, "BACKUP DEBUG: ", stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC)
		}
		b, err = backup.New(ctx, backupURI, logger)
		if err != nil {
			return fmt.Errorf("initialize backups: %v", err)
		}

		if loadBackups {
			loader, err = b.NewLoader(ctx)
			if err != nil {
				return fmt.Errorf("setup backup loader: %v", err)
			}
			// Allow backup object to get cleaned up after loading if we aren't using
			// it
			if !saveBackups {
				b = nil
			}
		}
	}

	err := os.MkdirAll(dbDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create data directory %s: %v", dbDir, err)
	}

	opts := deqdb.Options{
		Dir:                 dbDir,
		KeepCorrupt:         keepCorrupt,
		DefaultRequeueLimit: requeueLimit,
		UpgradeIfNeeded:     true,
		BackupLoader:        loader,
	}

	if debug {
		opts.Debug = stdlog.New(os.Stdout, "DEBUG: ", stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC)
	}

	store, err := deqdb.Open(opts)
	if err != nil {
		return fmt.Errorf("open database: %v", err)
	}
	defer store.Close()

	if saveBackups {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backupInterval):
				}

				err := b.Run(ctx, store)
				if err != nil {
					stdlog.Printf("run backup: %v", err)
					continue
				}
			}
		}()
	}

	server := handler.New(store)

	var grpcopts []grpc.ServerOption
	if authSharedSecret != "" {
		grpcopts = append(grpcopts,
			grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
				md, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					return nil, status.Error(codes.PermissionDenied, "no authorization token")
				}

				authorization := md.Get("authorization")
				if len(authorization) != 1 {
					return nil, status.Error(codes.PermissionDenied, "invalid authorization token")
				}

				if authorization[0] != authSharedSecret {
					return nil, status.Error(codes.PermissionDenied, "failed to validate authorization token")
				}
				return handler(ctx, req)
			}),
			grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				md, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					return status.Error(codes.PermissionDenied, "no authorization token")
				}

				authorization := md.Get("authorization")
				if len(authorization) != 1 {
					return status.Error(codes.PermissionDenied, "invalid authorization token")
				}

				if authorization[0] != authSharedSecret {
					return status.Error(codes.PermissionDenied, "failed to validate authorization token")
				}
				return handler(srv, ss)
			}),
		)
	} else {
		stdlog.Printf("WARNING: You are running DEQ without user authentication")
	}

	if !insecure {
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load tls credentials: %v", err)
		}
		grpcopts = append(grpcopts, grpc.Creds(creds))
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
			stdlog.Printf("received signal %v: shutting down...", s)
			grpcServer.Stop()
		}
	}()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("bind %s: %v", address, err)
	}

	stdlog.Printf("gRPC server listening on %s", address)

	err = grpcServer.Serve(lis)
	if err != nil {
		return fmt.Errorf("gRPC server failed: %v", err)
	}

	return nil
}

// func newBucket(endpoint, bucket, accessKey, secretKey, objectPrefix string) (Bucket, error) {

// 	client, err := minio.New(endpoint, accessKey, secretKey, true)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &minioBucket{client, bucket}
// }

// type Bucket interface {
// 	PutObject(ctx context.Context, name string, opts minio.PutObjectOptions) (io.WriteCloser, error)
// 	GetObject(ctx context.Context, name, file string, opts minio.GetObjectOptions) (io.ReadCloser, error)
// }

// type minioBucket struct {
// 	client *minio.Client
// 	bucket string
// }

// func (b *minioBucket) PutObject(ctx context.Context, name, file string, opts minio.PutObjectOptions) error {
// 	_, err := b.client.PutObjectWithContext(ctx, b.bucket, b.prefix + name, , )
// 	return err
// }

// func (b *minioBucket) GetObject(ctx context.Context, name, file string, opts minio.GetObjectOptions) error {
// 	err := b.client.FGetObjectWithContext(ctx, b.bucket, name, file, opts)
// 	return err
// }
