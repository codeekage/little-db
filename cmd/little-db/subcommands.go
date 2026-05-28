package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"little-db/internal/client"
	"little-db/internal/wire"
)

// ---------------------------------------------------------------------
// Common: connection flags + error classification
// ---------------------------------------------------------------------

// parseFlags is fs.Parse with support for flags appearing before,
// between, and after positional arguments. The stdlib flag package
// stops parsing at the first non-flag token, which silently turns a
// trailing `--value-stdin` into a positional VALUE — a sharp UX trap
// the package's own callers hit. This wrapper reorders args so flag
// position no longer matters; a literal "--" sentinel forces every
// following token to be positional (escape hatch for keys/values that
// genuinely start with `-`).
func parseFlags(fs *flag.FlagSet, args []string) error {
	var reordered, positional []string
	sawDoubleDash := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if sawDoubleDash {
			positional = append(positional, a)
			continue
		}
		if a == "--" {
			sawDoubleDash = true
			continue
		}
		// "-" by itself is the conventional stdin marker, not a flag.
		if len(a) > 1 && a[0] == '-' {
			reordered = append(reordered, a)
			if !strings.Contains(a, "=") {
				name := strings.TrimLeft(a, "-")
				if f := fs.Lookup(name); f != nil {
					bf, ok := f.Value.(interface{ IsBoolFlag() bool })
					if !(ok && bf.IsBoolFlag()) {
						if i+1 >= len(args) {
							// Value-expecting flag with nothing after it.
							// Hand straight to fs.Parse so it emits the
							// canonical "flag needs an argument" diagnostic.
							// Padding a "--" sentinel here would be silently
							// consumed as the missing value (e.g.
							//   little-db ping --addr  ->  addr="--").
							return fs.Parse(reordered)
						}
						// Refuse to consume the next token if it is itself
						// a known flag (e.g. `--addr --dial-timeout 1ms`).
						// Without this check the next flag was silently
						// absorbed as the value (addr="--dial-timeout"),
						// then --dial-timeout never reached fs.Parse and
						// 1ms became a positional. Hand off to fs.Parse so
						// the user sees the canonical missing-value error
						// against the right flag name.
						if isKnownFlagToken(fs, args[i+1]) {
							return fs.Parse(reordered)
						}
						reordered = append(reordered, args[i+1])
						i++
					}
				}
			}
			continue
		}
		positional = append(positional, a)
	}
	// Only emit the "--" sentinel when a positional could be mistaken
	// for a flag. Unconditionally appending it would let fs.Parse soak
	// it up as the value of a preceding value-expecting flag (handled
	// above), and it is a no-op otherwise.
	if needsFlagSentinel(positional) {
		reordered = append(reordered, "--")
	}
	reordered = append(reordered, positional...)
	return fs.Parse(reordered)
}

func needsFlagSentinel(positional []string) bool {
	for _, p := range positional {
		if len(p) > 0 && p[0] == '-' && p != "-" {
			return true
		}
	}
	return false
}

// isKnownFlagToken reports whether tok looks like a flag (starts with
// `-`, not bare `-`, not `--`) AND names a flag registered on fs. The
// `=value` form is stripped before lookup so `--addr=foo` matches.
func isKnownFlagToken(fs *flag.FlagSet, tok string) bool {
	if len(tok) < 2 || tok[0] != '-' || tok == "--" {
		return false
	}
	name := strings.TrimLeft(tok, "-")
	if eq := strings.IndexByte(name, '='); eq >= 0 {
		name = name[:eq]
	}
	return fs.Lookup(name) != nil
}

type connOpts struct {
	addr           string
	dialTimeout    time.Duration
	requestTimeout time.Duration
}

func registerConnFlags(fs *flag.FlagSet) *connOpts {
	co := &connOpts{}
	fs.StringVar(&co.addr, "addr",
		envOr("LITTLE_DB_ADDR", defaultAddr),
		"server address (env: LITTLE_DB_ADDR)")
	fs.DurationVar(&co.dialTimeout, "dial-timeout",
		envDurOr("LITTLE_DB_DIAL_TIMEOUT", defaultDialTimeout),
		"TCP dial timeout (env: LITTLE_DB_DIAL_TIMEOUT)")
	fs.DurationVar(&co.requestTimeout, "request-timeout",
		envDurOr("LITTLE_DB_REQUEST_TIMEOUT", defaultRequestTimeout),
		"per-request deadline (env: LITTLE_DB_REQUEST_TIMEOUT)")
	return co
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDurOr(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func dialClient(co *connOpts, stderr io.Writer) (*client.Client, int) {
	c, err := client.Dial(co.addr, client.Options{
		DialTimeout:    co.dialTimeout,
		RequestTimeout: co.requestTimeout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "little-db: dial %s: %v\n", co.addr, err)
		return nil, exitTransport
	}
	return c, exitOK
}

// classifyErr maps a client error to a CLI exit code and prints a
// human-readable diagnostic to stderr. Returns exitOK for nil.
func classifyErr(err error, stderr io.Writer) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, client.ErrNotFound) {
		fmt.Fprintln(stderr, "little-db: not found")
		return exitNotFound
	}
	var re *wire.RemoteError
	if errors.As(err, &re) {
		fmt.Fprintf(stderr, "little-db: server returned %s: %s\n",
			statusName(re.Status), re.Msg)
		switch re.Status {
		case wire.StatusNotFound:
			return exitNotFound
		case wire.StatusBadRequest, wire.StatusInternal:
			return exitRemoteBad
		case wire.StatusOverload, wire.StatusClosed:
			return exitOverloaded
		case wire.StatusFollowerReadOnly:
			// Operator pointed a write at a follower. Treat as
			// configuration error rather than transient overload
			// so scripts that retry on OVERLOAD don't busy-loop;
			// the leader-hint message in re.Msg already tells the
			// operator where to send the write.
			return exitRemoteBad
		default:
			return exitRemoteBad
		}
	}
	var pe *wire.ProtocolError
	if errors.As(err, &pe) {
		fmt.Fprintf(stderr, "little-db: protocol error: %v\n", err)
		return exitTransport
	}
	var fe *wire.FrameError
	if errors.As(err, &fe) {
		fmt.Fprintf(stderr, "little-db: frame error: %v\n", err)
		return exitTransport
	}
	fmt.Fprintf(stderr, "little-db: %v\n", err)
	return exitTransport
}

func statusName(s wire.Status) string {
	switch s {
	case wire.StatusOK:
		return "OK"
	case wire.StatusNotFound:
		return "NOT_FOUND"
	case wire.StatusBadRequest:
		return "BAD_REQUEST"
	case wire.StatusInternal:
		return "INTERNAL"
	case wire.StatusClosed:
		return "CLOSED"
	case wire.StatusOverload:
		return "OVERLOAD"
	case wire.StatusFollowerReadOnly:
		return "FOLLOWER_READ_ONLY"
	default:
		return fmt.Sprintf("status(0x%02x)", uint8(s))
	}
}

// decodeArg returns the raw bytes for a CLI positional argument.
// If hex is true, arg is hex-decoded; otherwise its UTF-8 bytes are used.
func decodeArg(arg string, asHex bool, label string) ([]byte, error) {
	if asHex {
		b, err := hex.DecodeString(arg)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid hex: %v", label, err)
		}
		return b, nil
	}
	return []byte(arg), nil
}

// validateKey enforces the wire-level key constraints (non-empty,
// <= MaxKeyLen) locally so a bad CLI argument exits with exitUsage
// instead of round-tripping to the server and returning exitTransport
// via *wire.ProtocolError.
func validateKey(key []byte, label string) error {
	if len(key) == 0 {
		return fmt.Errorf("%s: empty", label)
	}
	if len(key) > wire.MaxKeyLen {
		return fmt.Errorf("%s: %d bytes exceeds max=%d", label, len(key), wire.MaxKeyLen)
	}
	return nil
}

// validateValue enforces the wire-level value cap locally. A nil value
// is allowed (engine accepts zero-length values for PUT).
func validateValue(val []byte, label string) error {
	if len(val) > wire.MaxValLen {
		return fmt.Errorf("%s: %d bytes exceeds max=%d", label, len(val), wire.MaxValLen)
	}
	return nil
}

// validateRangeBound allows an empty bound (means "unbounded") but caps
// non-empty bounds at MaxKeyLen.
func validateRangeBound(b []byte, label string) error {
	if len(b) > wire.MaxKeyLen {
		return fmt.Errorf("%s: %d bytes exceeds max=%d", label, len(b), wire.MaxKeyLen)
	}
	return nil
}

// ---------------------------------------------------------------------
// put
// ---------------------------------------------------------------------

func runPut(args []string, stdin io.Reader, stderr io.Writer) int {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	keyHex := fs.Bool("key-hex", false, "treat KEY positional as hex")
	valHex := fs.Bool("value-hex", false, "treat VALUE positional as hex")
	valStdin := fs.Bool("value-stdin", false,
		"read VALUE bytes from stdin (raw, no decoding); when set, omit the VALUE positional")
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	wantArgs := 2
	if *valStdin {
		wantArgs = 1
		if *valHex {
			fmt.Fprintln(stderr, "put: --value-stdin and --value-hex are mutually exclusive")
			return exitUsage
		}
	}
	if fs.NArg() != wantArgs {
		if *valStdin {
			fmt.Fprintln(stderr, "put --value-stdin: expected KEY (value comes from stdin)")
		} else {
			fmt.Fprintln(stderr, "put: expected KEY VALUE")
		}
		return exitUsage
	}
	key, err := decodeArg(fs.Arg(0), *keyHex, "key")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if err := validateKey(key, "key"); err != nil {
		fmt.Fprintln(stderr, "put:", err)
		return exitUsage
	}
	var val []byte
	if *valStdin {
		val, err = io.ReadAll(io.LimitReader(stdin, int64(wire.MaxValLen)+1))
		if err != nil {
			fmt.Fprintf(stderr, "put --value-stdin: read: %v\n", err)
			return exitTransport
		}
	} else {
		val, err = decodeArg(fs.Arg(1), *valHex, "value")
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	if err := validateValue(val, "value"); err != nil {
		fmt.Fprintln(stderr, "put:", err)
		return exitUsage
	}
	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	if code := classifyErr(c.Put(key, val), stderr); code != exitOK {
		return code
	}
	fmt.Fprintf(stderr, "OK put key=%d val=%d\n", len(key), len(val))
	return exitOK
}

// ---------------------------------------------------------------------
// get
// ---------------------------------------------------------------------

func runGet(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	keyHex := fs.Bool("key-hex", false, "treat KEY positional as hex")
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "get: expected KEY")
		return exitUsage
	}
	key, err := decodeArg(fs.Arg(0), *keyHex, "key")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if err := validateKey(key, "key"); err != nil {
		fmt.Fprintln(stderr, "get:", err)
		return exitUsage
	}
	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	val, err := c.Get(key)
	if err != nil {
		// Surface the missing key in the diagnostic — "not found"
		// with no context forces the operator to scroll back through
		// shell history to remember what they asked for. %q quotes
		// safely so binary keys don't garble the terminal.
		if errors.Is(err, client.ErrNotFound) {
			fmt.Fprintf(stderr, "little-db get: no such key %q\n", key)
			return exitNotFound
		}
		return classifyErr(err, stderr)
	}
	if _, werr := stdout.Write(val); werr != nil {
		fmt.Fprintf(stderr, "get: write stdout: %v\n", werr)
		return exitTransport
	}
	return exitOK
}

// ---------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------

func runDelete(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	keyHex := fs.Bool("key-hex", false, "treat KEY positional as hex")
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "delete: expected KEY")
		return exitUsage
	}
	key, err := decodeArg(fs.Arg(0), *keyHex, "key")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if err := validateKey(key, "key"); err != nil {
		fmt.Fprintln(stderr, "delete:", err)
		return exitUsage
	}
	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	if code := classifyErr(c.Delete(key), stderr); code != exitOK {
		return code
	}
	fmt.Fprintf(stderr, "OK delete key=%d\n", len(key))
	return exitOK
}

// ---------------------------------------------------------------------
// ping
// ---------------------------------------------------------------------

func runPing(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ping", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "ping: takes no positional args")
		return exitUsage
	}
	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		return classifyErr(err, stderr)
	}
	fmt.Fprintln(stdout, "pong")
	return exitOK
}

// ---------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------

func runStats(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "stats: takes no positional args")
		return exitUsage
	}
	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	keys, bytes, err := c.Stats()
	if err != nil {
		return classifyErr(err, stderr)
	}
	fmt.Fprintf(stdout, "key_count=%d bytes_on_disk=%d\n", keys, bytes)
	return exitOK
}

// ---------------------------------------------------------------------
// range
// ---------------------------------------------------------------------

func runRange(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("range", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	startStr := fs.String("start", "", "range start (inclusive); empty = from first key")
	endStr := fs.String("end", "", "range end (exclusive); empty = to last key")
	startHex := fs.Bool("start-hex", false, "treat --start as hex")
	endHex := fs.Bool("end-hex", false, "treat --end as hex")
	format := fs.String("format", "ndjson",
		`output format: "ndjson" (default) or "raw" (KEY\tVALUE\n)`)
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, `range: use --start/--end flags, not positional args`)
		return exitUsage
	}
	var start, end []byte
	if *startStr != "" {
		b, err := decodeArg(*startStr, *startHex, "start")
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		if err := validateRangeBound(b, "start"); err != nil {
			fmt.Fprintln(stderr, "range:", err)
			return exitUsage
		}
		start = b
	}
	if *endStr != "" {
		b, err := decodeArg(*endStr, *endHex, "end")
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		if err := validateRangeBound(b, "end"); err != nil {
			fmt.Fprintln(stderr, "range:", err)
			return exitUsage
		}
		end = b
	}
	// Wire rejects start > end as a *ProtocolError; surface it locally
	// as exitUsage instead of round-tripping to exitTransport.
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0 {
		fmt.Fprintf(stderr, "range: --start (%q) > --end (%q)\n", start, end)
		return exitUsage
	}

	var emit func(kv wire.KV) error
	switch *format {
	case "ndjson":
		emit = func(kv wire.KV) error {
			line := struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				Key:   base64.StdEncoding.EncodeToString(kv.Key),
				Value: base64.StdEncoding.EncodeToString(kv.Value),
			}
			b, err := json.Marshal(line)
			if err != nil {
				return err
			}
			b = append(b, '\n')
			_, err = stdout.Write(b)
			return err
		}
	case "raw":
		emit = func(kv wire.KV) error {
			// KEY\tVALUE\n — caller must guarantee no tabs/newlines
			// in keys/values for this format to be lossless.
			if _, err := stdout.Write(kv.Key); err != nil {
				return err
			}
			if _, err := io.WriteString(stdout, "\t"); err != nil {
				return err
			}
			if _, err := stdout.Write(kv.Value); err != nil {
				return err
			}
			_, err := io.WriteString(stdout, "\n")
			return err
		}
	default:
		fmt.Fprintf(stderr, "range: unknown --format %q (want ndjson|raw)\n", *format)
		return exitUsage
	}

	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()

	var writeErr error
	err := c.ReadKeyRange(start, end, func(pairs []wire.KV) bool {
		for i := range pairs {
			if werr := emit(pairs[i]); werr != nil {
				writeErr = werr
				return false
			}
		}
		return true
	})
	if writeErr != nil {
		fmt.Fprintf(stderr, "range: write stdout: %v\n", writeErr)
		return exitTransport
	}
	return classifyErr(err, stderr)
}

// ---------------------------------------------------------------------
// batch
// ---------------------------------------------------------------------

type batchLine struct {
	Op    string `json:"op"`              // "put" or "delete"
	Key   string `json:"key"`             // base64 by default, literal UTF-8 when --plain
	Value string `json:"value,omitempty"` // base64 by default, literal UTF-8 when --plain (put only)
}

// decodeBatchField decodes a batch key or value field according to mode.
// In default mode the field is base64 (binary-safe). With --plain the
// field is taken as a literal UTF-8 string — convenient for hand-edited
// fixtures, but cannot carry arbitrary bytes.
func decodeBatchField(s string, plain bool) ([]byte, error) {
	if plain {
		return []byte(s), nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("not valid base64: %w (use --plain for literal strings)", err)
	}
	return b, nil
}

func runBatch(args []string, stdin io.Reader, stderr io.Writer) int {
	fs := flag.NewFlagSet("batch", flag.ContinueOnError)
	fs.SetOutput(stderr)
	co := registerConnFlags(fs)
	plain := fs.Bool("plain", false,
		"interpret key/value as literal JSON strings (UTF-8 only); default is base64 (binary-safe)")
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, `batch: expected one positional arg ("-" for stdin)`)
		return exitUsage
	}
	src := fs.Arg(0)
	var r io.Reader
	switch src {
	case "-":
		r = stdin
	default:
		f, err := os.Open(src)
		if err != nil {
			fmt.Fprintf(stderr, "batch: open %s: %v\n", src, err)
			return exitUsage
		}
		defer f.Close()
		r = f
	}

	var entries []wire.BatchEntry
	// Accumulate the encoded BATCH body size so we can reject a batch
	// that would exceed wire.MaxFramePayload locally (exitUsage) rather
	// than dialing, encoding, and reporting *wire.FrameError as a
	// transport failure. Body shape:
	//   u32 count | repeat: (u8 op | u32 klen | key | u32 vlen | value)
	// Frame payload budget is MaxFramePayload - 1 (tag byte).
	const batchEntryOverhead = 1 + 4 + 4 // op + klen + vlen
	const batchHeaderSize = 4            // count
	const batchBodyBudget = wire.MaxFramePayload - 1
	encodedSize := batchHeaderSize
	// Stream-decode NDJSON so single objects can be larger than any
	// fixed buffer. Each PUT entry's value can reach wire.MaxValLen
	// (~16 MiB raw, ~22.4 MiB base64'd, more with JSON overhead) — a
	// bufio.Scanner with a fixed cap can't ingest that.
	dec := json.NewDecoder(r)
	lineNo := 0
	for {
		lineNo++
		var l batchLine
		if err := dec.Decode(&l); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(stderr, "batch: entry %d: invalid JSON: %v\n", lineNo, err)
			return exitUsage
		}
		key, err := decodeBatchField(l.Key, *plain)
		if err != nil {
			fmt.Fprintf(stderr, "batch: entry %d: key %v\n", lineNo, err)
			return exitUsage
		}
		if err := validateKey(key, "key"); err != nil {
			fmt.Fprintf(stderr, "batch: entry %d: %v\n", lineNo, err)
			return exitUsage
		}
		switch l.Op {
		case "put":
			val, err := decodeBatchField(l.Value, *plain)
			if err != nil {
				fmt.Fprintf(stderr, "batch: entry %d: value %v\n", lineNo, err)
				return exitUsage
			}
			if err := validateValue(val, "value"); err != nil {
				fmt.Fprintf(stderr, "batch: entry %d: %v\n", lineNo, err)
				return exitUsage
			}
			encodedSize += batchEntryOverhead + len(key) + len(val)
			if encodedSize > batchBodyBudget {
				fmt.Fprintf(stderr,
					"batch: encoded size after entry %d exceeds max frame payload (%d > %d)\n",
					lineNo, encodedSize, batchBodyBudget)
				return exitUsage
			}
			entries = append(entries, wire.BatchEntry{Key: key, Value: val})
		case "delete":
			encodedSize += batchEntryOverhead + len(key)
			if encodedSize > batchBodyBudget {
				fmt.Fprintf(stderr,
					"batch: encoded size after entry %d exceeds max frame payload (%d > %d)\n",
					lineNo, encodedSize, batchBodyBudget)
				return exitUsage
			}
			entries = append(entries, wire.BatchEntry{Key: key, Delete: true})
		default:
			fmt.Fprintf(stderr, `batch: entry %d: op must be "put" or "delete", got %q`+"\n",
				lineNo, l.Op)
			return exitUsage
		}
	}
	if len(entries) == 0 {
		fmt.Fprintln(stderr, "batch: no entries to submit")
		return exitUsage
	}
	if len(entries) > wire.MaxBatchEntries {
		fmt.Fprintf(stderr, "batch: %d entries exceeds max=%d\n",
			len(entries), wire.MaxBatchEntries)
		return exitUsage
	}

	c, code := dialClient(co, stderr)
	if code != exitOK {
		return code
	}
	defer c.Close()
	if code := classifyErr(c.Batch(entries), stderr); code != exitOK {
		return code
	}
	fmt.Fprintf(stderr, "OK batch entries=%d\n", len(entries))
	return exitOK
}
