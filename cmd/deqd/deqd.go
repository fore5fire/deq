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
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/backup"
	"gitlab.com/katcheCode/deq/cmd/deqd/internal/handler"
	"gitlab.com/katcheCode/deq/deqdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	// backupMode sets the deqd backup behavior. Valid options are "load_only",
	// "backup_only", "sync", or "disabled". Defaults to "disabled".
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.LUTC)
	log.SetPrefix("")
}

func main() {
	log.Println("Starting up")

	if limit, ok := os.LookupEnv("DEQ_DEFAULT_REQUEUE_LIMIT"); ok {
		var err error
		requeueLimit, err = strconv.Atoi(limit)
		if err != nil {
			log.Fatalf("parse DEQ_DEFAULT_REQUEUE_LIMIT from environment: %v", err)
		}
	}

	if backupIntervalSeconds != "" {
		seconds, err := strconv.ParseInt(backupIntervalSeconds, 10, 32)
		if err != nil {
			log.Fatalf("parse DEQ_BACKUP_INTERVAL_SECONDS: %v", err)
		}
		backupInterval = time.Duration(seconds) * time.Second
	}

	if dataDir == "" {
		dataDir = "/var/deqd"
	}
	if listenAddress == "" {
		listenAddress = ":8080"
	}

	// run start code in seperate function so we can both defer and os.Exit
	err := run(dataDir, listenAddress, statsAddress, certFile, keyFile, insecure)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("graceful shutdown complete")
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

	var b *backup.Backup
	var loader deqdb.Loader
	loadBackups := strings.EqualFold(backupMode, "load_only") || strings.EqualFold(backupMode, "sync")
	saveBackups := strings.EqualFold(backupMode, "backup_only") || strings.EqualFold(backupMode, "sync")
	if loadBackups || saveBackups {
		var logger backup.Logger
		var err error
		if debug {
			logger = log.New(os.Stdout, "BACKUP DEBUG: ", log.Ltime|log.Lmicroseconds|log.LUTC)
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
		opts.Debug = log.New(os.Stdout, "DEBUG: ", log.Ltime|log.Lmicroseconds|log.LUTC)
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
					log.Printf("run backup: %v", err)
					continue
				}
			}
		}()
	}

	server := handler.New(store)

	var grpcopts []grpc.ServerOption

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
			log.Printf("received signal %v: shutting down...", s)
			grpcServer.Stop()
		}
	}()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("bind %s: %v", address, err)
	}

	log.Printf("gRPC server listening on %s", address)

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
