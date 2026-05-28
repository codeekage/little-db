// Package main is the little-db CLI binary. It exposes one server
// subcommand (`serve`) and a set of data-plane subcommands
// (`put`/`get`/`delete`/`range`/`batch`/`stats`/`ping`) that drive the
// TCP server over the Go client.
//
// Exit codes:
//
//	0  success
//	1  transport / I/O error (connect refused, broken pipe, deadline, etc.)
//	2  bad usage (unknown subcommand, bad flag, malformed argument)
//	3  remote BAD_REQUEST or INTERNAL
//	4  remote NOT_FOUND (Get / Delete returning a missing key)
//	5  remote OVERLOAD or CLOSED
//
// The package keeps an internal `run(args, stdin, stdout, stderr) int`
// entry point so end-to-end tests can drive the CLI in-process without
// spawning a subprocess.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"little-db/internal/engine"
	"little-db/internal/logging"
	"little-db/internal/server"
)

const (
	exitOK         = 0
	exitTransport  = 1
	exitUsage      = 2
	exitRemoteBad  = 3
	exitNotFound   = 4
	exitOverloaded = 5

	defaultAddr           = "127.0.0.1:4242"
	defaultDialTimeout    = 5 * time.Second
	defaultRequestTimeout = 30 * time.Second
	defaultShutdownGrace  = 30 * time.Second
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run is the in-process entry point. It returns the process exit code so
// tests can drive the CLI without os/exec.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return exitUsage
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "serve":
		return runServe(rest, stdout, stderr)
	case "put":
		return runPut(rest, stdin, stderr)
	case "get":
		return runGet(rest, stdout, stderr)
	case "delete":
		return runDelete(rest, stderr)
	case "range":
		return runRange(rest, stdout, stderr)
	case "batch":
		return runBatch(rest, stdin, stderr)
	case "stats":
		return runStats(rest, stdout, stderr)
	case "ping":
		return runPing(rest, stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stdout)
		return exitOK
	default:
		fmt.Fprintf(stderr, "little-db: unknown subcommand %q\n", cmd)
		printUsage(stderr)
		return exitUsage
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: little-db <subcommand> [flags]

Subcommands:
  serve    Run the TCP server, bound to an on-disk engine
  put      PUT a key=value
  get      GET a key
  delete   DELETE a key
  range    Stream a key range
  batch    Submit a batch (NDJSON on stdin with "-")
  stats    Print server stats
  ping     Health-check the server

Run "little-db <subcommand> --help" for subcommand flags.`)
}

// ---------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------

func runServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)

	// Server / network knobs.
	addr := fs.String("addr", defaultAddr, "TCP listen address")
	readDeadline := fs.Duration("read-deadline", 30*time.Second,
		"per-request read deadline")
	writeDeadline := fs.Duration("write-deadline", 30*time.Second,
		"per-response write deadline")
	maxRangeStreams := fs.Int("max-concurrent-range-streams", 4,
		"server-wide cap on in-flight range streams")
	maxRangeBytes := fs.Int64("max-range-response-bytes", 64*1024*1024,
		"per-range payload byte cap")
	shutdownGrace := fs.Duration("shutdown-grace", defaultShutdownGrace,
		"max time to wait for graceful shutdown after a signal")

	// Engine knobs.
	dataDir := fs.String("data-dir", "", "engine data directory (required)")
	maxSegmentSize := fs.Int64("max-segment-size", 256*1024*1024,
		"segment rotation threshold in bytes")
	syncOnPut := fs.Bool("sync-on-put", false,
		"fsync after every write (durable but slow)")
	writeQueueDepth := fs.Int("write-queue-depth", 64,
		"engine writer request channel depth")
	maxBatchEncoded := fs.Int64("max-batch-encoded-size", 64*1024*1024,
		"max encoded batch size in bytes")
	compactionInterval := fs.Duration("compaction-interval", 0,
		"background compaction interval (0 disables)")

	// Observability knobs.
	logLevel := fs.String("log-level", "info",
		"log level: debug|info|warn|error (debug enables per-request logs)")
	logFormat := fs.String("log-format", "text",
		"log format: text|json")

	if err := parseFlags(fs, args); err != nil {
		// flag has already printed the diagnostic.
		return exitUsage
	}
	if *dataDir == "" {
		fmt.Fprintln(stderr, "little-db serve: --data-dir is required")
		return exitUsage
	}

	lvl, err := logging.ParseLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(stderr, "little-db serve: %v\n", err)
		return exitUsage
	}
	format, err := logging.ParseFormat(*logFormat)
	if err != nil {
		fmt.Fprintf(stderr, "little-db serve: %v\n", err)
		return exitUsage
	}
	logger := logging.New(stderr, lvl, format)

	db, err := engine.Open(engine.Options{
		Dir:                 *dataDir,
		MaxSegmentSize:      *maxSegmentSize,
		SyncOnPut:           *syncOnPut,
		WriteQueueDepth:     *writeQueueDepth,
		MaxBatchEncodedSize: *maxBatchEncoded,
		CompactionInterval:  *compactionInterval,
		Logger:              logger,
	})
	if err != nil {
		fmt.Fprintf(stderr, "little-db serve: open engine: %v\n", err)
		return exitTransport
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			fmt.Fprintf(stderr, "little-db serve: close engine: %v\n", cerr)
		}
	}()

	srv := server.New(db, server.Options{
		Addr:                      *addr,
		ReadDeadline:              *readDeadline,
		WriteDeadline:             *writeDeadline,
		MaxConcurrentRangeStreams: *maxRangeStreams,
		MaxRangeResponseBytes:     *maxRangeBytes,
		Logger:                    logger,
	})
	if err := srv.Bind(); err != nil {
		fmt.Fprintf(stderr, "little-db serve: bind %s: %v\n", *addr, err)
		return exitTransport
	}
	fmt.Fprintf(stdout, "little-db: listening on %s (data-dir=%s)\n",
		srv.Addr(), *dataDir)

	// Serve in a goroutine; signal handler triggers Shutdown.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		// Signal received. Stop signal handler so a second Ctrl-C
		// hard-kills the process instead of being absorbed.
		stop()
		fmt.Fprintln(stdout, "little-db: shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			fmt.Fprintf(stderr, "little-db serve: shutdown: %v\n", err)
			// Drain Serve's return so the goroutine exits cleanly.
			<-serveErr
			return exitTransport
		}
		<-serveErr
		return exitOK
	case err := <-serveErr:
		// Serve returned on its own — listener died unexpectedly.
		if err != nil {
			fmt.Fprintf(stderr, "little-db serve: %v\n", err)
			return exitTransport
		}
		return exitOK
	}
}
