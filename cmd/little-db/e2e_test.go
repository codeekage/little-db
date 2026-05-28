package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"little-db/internal/engine"
	"little-db/internal/server"
	"little-db/internal/wire"
)

// startTestServer brings up an in-process server on 127.0.0.1:0 backed
// by a fresh engine directory under t.TempDir(). Returns the bound TCP
// address as "host:port".
func startTestServer(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	db, err := engine.Open(engine.Options{Dir: dir})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	srv := server.New(db, server.Options{
		Addr:          "127.0.0.1:0",
		ReadDeadline:  5 * time.Second,
		WriteDeadline: 5 * time.Second,
	})
	if err := srv.Bind(); err != nil {
		t.Fatalf("srv.Bind: %v", err)
	}
	addr := srv.Addr().String()
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Logf("srv.Shutdown: %v", err)
		}
		<-done
		if err := db.Close(); err != nil {
			t.Logf("db.Close: %v", err)
		}
	})
	return addr
}

// invoke runs the CLI in-process with the given args and stdin. Returns
// stdout, stderr, exit code.
func invoke(args []string, stdin string) (string, string, int) {
	var out, errb bytes.Buffer
	code := run(args, strings.NewReader(stdin), &out, &errb)
	return out.String(), errb.String(), code
}

func TestE2EPingPutGetDeleteStats(t *testing.T) {
	addr := startTestServer(t)
	mk := func(rest ...string) []string {
		return append([]string{rest[0], "--addr", addr}, rest[1:]...)
	}

	// ping
	out, errb, code := invoke(mk("ping"), "")
	if code != exitOK {
		t.Fatalf("ping: code=%d stderr=%q", code, errb)
	}
	if strings.TrimSpace(out) != "pong" {
		t.Fatalf("ping stdout = %q", out)
	}

	// put
	_, errb, code = invoke(mk("put", "hello", "world"), "")
	if code != exitOK {
		t.Fatalf("put: code=%d stderr=%q", code, errb)
	}

	// get
	out, errb, code = invoke(mk("get", "hello"), "")
	if code != exitOK {
		t.Fatalf("get: code=%d stderr=%q", code, errb)
	}
	if out != "world" {
		t.Fatalf("get stdout = %q want %q", out, "world")
	}

	// stats — at least one key, nonzero bytes
	out, errb, code = invoke(mk("stats"), "")
	if code != exitOK {
		t.Fatalf("stats: code=%d stderr=%q", code, errb)
	}
	if !strings.Contains(out, "key_count=") || !strings.Contains(out, "bytes_on_disk=") {
		t.Fatalf("stats stdout = %q", out)
	}

	// delete
	_, errb, code = invoke(mk("delete", "hello"), "")
	if code != exitOK {
		t.Fatalf("delete: code=%d stderr=%q", code, errb)
	}

	// get-after-delete → exit 4
	_, _, code = invoke(mk("get", "hello"), "")
	if code != exitNotFound {
		t.Fatalf("get-after-delete: code=%d want %d", code, exitNotFound)
	}
}

func TestE2ERangeNDJSONAndRaw(t *testing.T) {
	addr := startTestServer(t)
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		_, errb, code := invoke([]string{"put", "--addr", addr, kv[0], kv[1]}, "")
		if code != exitOK {
			t.Fatalf("put %s: code=%d stderr=%q", kv[0], code, errb)
		}
	}

	// NDJSON (default)
	out, errb, code := invoke([]string{"range", "--addr", addr}, "")
	if code != exitOK {
		t.Fatalf("range ndjson: code=%d stderr=%q", code, errb)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("ndjson lines = %d want 3: %q", len(lines), out)
	}
	wantKeys := []string{"a", "b", "c"}
	wantVals := []string{"1", "2", "3"}
	for i, line := range lines {
		var rec struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("ndjson line %d: parse %q: %v", i, line, err)
		}
		k, _ := base64.StdEncoding.DecodeString(rec.Key)
		v, _ := base64.StdEncoding.DecodeString(rec.Value)
		if string(k) != wantKeys[i] || string(v) != wantVals[i] {
			t.Fatalf("ndjson line %d: key=%q value=%q", i, k, v)
		}
	}

	// raw
	out, errb, code = invoke([]string{"range", "--addr", addr, "--format", "raw"}, "")
	if code != exitOK {
		t.Fatalf("range raw: code=%d stderr=%q", code, errb)
	}
	want := "a\t1\nb\t2\nc\t3\n"
	if out != want {
		t.Fatalf("range raw stdout = %q want %q", out, want)
	}

	// bounded range: --start b → b, c
	out, errb, code = invoke([]string{
		"range", "--addr", addr, "--format", "raw", "--start", "b",
	}, "")
	if code != exitOK {
		t.Fatalf("range bounded: code=%d stderr=%q", code, errb)
	}
	if out != "b\t2\nc\t3\n" {
		t.Fatalf("range bounded stdout = %q", out)
	}
}

func TestE2EBatchStdinNDJSON(t *testing.T) {
	addr := startTestServer(t)
	mkLine := func(op, key, val string) string {
		k := base64.StdEncoding.EncodeToString([]byte(key))
		if op == "delete" {
			return fmt.Sprintf(`{"op":"delete","key":%q}`+"\n", k)
		}
		v := base64.StdEncoding.EncodeToString([]byte(val))
		return fmt.Sprintf(`{"op":"put","key":%q,"value":%q}`+"\n", k, v)
	}
	stdin := mkLine("put", "x", "X") +
		mkLine("put", "y", "Y") +
		mkLine("delete", "x", "")

	_, errb, code := invoke([]string{"batch", "--addr", addr, "-"}, stdin)
	if code != exitOK {
		t.Fatalf("batch: code=%d stderr=%q", code, errb)
	}
	// x deleted → not found; y present
	_, _, code = invoke([]string{"get", "--addr", addr, "x"}, "")
	if code != exitNotFound {
		t.Fatalf("get x after batch: code=%d want %d", code, exitNotFound)
	}
	out, _, code := invoke([]string{"get", "--addr", addr, "y"}, "")
	if code != exitOK || out != "Y" {
		t.Fatalf("get y after batch: code=%d out=%q", code, out)
	}
}

func TestE2EUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no-args", nil},
		{"unknown-subcommand", []string{"frobnicate"}},
		{"put-missing-value", []string{"put", "k"}},
		{"get-no-key", []string{"get"}},
		{"range-unknown-format", []string{"range", "--format", "yaml"}},
		{"batch-no-src", []string{"batch"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, code := invoke(tc.args, "")
			if code != exitUsage {
				t.Fatalf("code=%d want exitUsage=%d", code, exitUsage)
			}
		})
	}
}

func TestE2ETransportErrorOnBadAddr(t *testing.T) {
	// 127.0.0.1:1 is reserved (tcpmux) and almost certainly refused.
	_, _, code := invoke([]string{
		"ping", "--addr", "127.0.0.1:1", "--dial-timeout", "500ms",
	}, "")
	if code == exitOK {
		t.Skip("something is actually listening on 127.0.0.1:1; skipping")
	}
	if code != exitTransport {
		t.Fatalf("code=%d want exitTransport=%d", code, exitTransport)
	}
}

// TestE2EPrevalidationExitCodes pins the fix for the medium finding:
// local request-shape errors (empty key, oversize key/value) must exit
// with exitUsage (2), not exitTransport (1). The CLI must not dial.
func TestE2EPrevalidationExitCodes(t *testing.T) {
	// Use a bogus addr so any dial would fail fast — if the CLI ever
	// regresses to dialing before validating, this test would still see
	// exitTransport instead of exitUsage and fail loudly.
	const badAddr = "127.0.0.1:1"
	bigKey := strings.Repeat("k", wire.MaxKeyLen+1)
	bigVal := strings.Repeat("v", wire.MaxValLen+1)

	cases := []struct {
		name string
		args []string
	}{
		{"put-empty-key", []string{"put", "--addr", badAddr, "", "v"}},
		{"put-oversize-key", []string{"put", "--addr", badAddr, bigKey, "v"}},
		{"put-oversize-value", []string{"put", "--addr", badAddr, "k", bigVal}},
		{"get-empty-key", []string{"get", "--addr", badAddr, ""}},
		{"get-oversize-key", []string{"get", "--addr", badAddr, bigKey}},
		{"delete-empty-key", []string{"delete", "--addr", badAddr, ""}},
		{"delete-oversize-key", []string{"delete", "--addr", badAddr, bigKey}},
		{"range-oversize-start", []string{"range", "--addr", badAddr, "--start", bigKey}},
		{"range-oversize-end", []string{"range", "--addr", badAddr, "--end", bigKey}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, code := invoke(tc.args, "")
			if code != exitUsage {
				t.Fatalf("code=%d want exitUsage=%d", code, exitUsage)
			}
		})
	}
}

// TestE2EPutValueStdin pins the new --value-stdin flag.
func TestE2EPutValueStdin(t *testing.T) {
	addr := startTestServer(t)
	val := strings.Repeat("Z", 1<<20) // 1 MiB
	_, errb, code := invoke([]string{
		"put", "--addr", addr, "--value-stdin", "bigkey",
	}, val)
	if code != exitOK {
		t.Fatalf("put --value-stdin: code=%d stderr=%q", code, errb)
	}
	out, _, code := invoke([]string{"get", "--addr", addr, "bigkey"}, "")
	if code != exitOK {
		t.Fatalf("get: code=%d", code)
	}
	if out != val {
		t.Fatalf("get stdout len=%d want %d", len(out), len(val))
	}

	// --value-stdin conflicts with --value-hex.
	_, _, code = invoke([]string{
		"put", "--addr", addr, "--value-stdin", "--value-hex", "k",
	}, "")
	if code != exitUsage {
		t.Fatalf("value-stdin+value-hex: code=%d want exitUsage", code)
	}
}

// TestE2EBatchOversizeValueRegression pins the json.Decoder switch:
// a single NDJSON entry whose base64-encoded value+JSON overhead exceeds
// what the prior bufio.Scanner cap (16 MiB) could ingest must still go
// through. We use a value at wire.MaxValLen / 2 to keep the test under a
// few seconds on disk while still encoding to > 16 MiB of NDJSON line.
func TestE2EBatchOversizeValueRegression(t *testing.T) {
	addr := startTestServer(t)
	// 12 MiB raw -> 16 MiB base64 -> > 16 MiB NDJSON line after JSON
	// overhead. The old bufio.Scanner Buffer(64KiB, 16MiB) cap would
	// have rejected this with "bufio.Scanner: token too long".
	const rawSize = 12 * 1024 * 1024
	val := bytes.Repeat([]byte{'V'}, rawSize)
	k := base64.StdEncoding.EncodeToString([]byte("big"))
	v := base64.StdEncoding.EncodeToString(val)
	line := fmt.Sprintf(`{"op":"put","key":%q,"value":%q}`+"\n", k, v)
	if len(line) <= 16*1024*1024 {
		t.Fatalf("regression precondition: line len=%d, want > 16 MiB", len(line))
	}
	_, errb, code := invoke([]string{"batch", "--addr", addr, "-"}, line)
	if code != exitOK {
		t.Fatalf("batch oversize: code=%d stderr=%q", code, errb)
	}
	// Verify it stored correctly via stats — full Get of a 12 MiB
	// value would dominate test time without proving anything new.
	out, _, code := invoke([]string{"stats", "--addr", addr}, "")
	if code != exitOK {
		t.Fatalf("stats: code=%d", code)
	}
	if !strings.Contains(out, "key_count=1") {
		t.Fatalf("stats: %q", out)
	}
}

// TestE2EBatchEntryValidation pins prevalidation for batch entries —
// empty/oversize keys and oversize values must surface as exitUsage,
// not transport errors.
func TestE2EBatchEntryValidation(t *testing.T) {
	const badAddr = "127.0.0.1:1"
	emptyKeyLine := fmt.Sprintf(`{"op":"put","key":"","value":%q}`+"\n",
		base64.StdEncoding.EncodeToString([]byte("v")))
	bigKeyB64 := base64.StdEncoding.EncodeToString(
		bytes.Repeat([]byte{'k'}, wire.MaxKeyLen+1))
	bigKeyLine := fmt.Sprintf(`{"op":"put","key":%q,"value":%q}`+"\n",
		bigKeyB64, base64.StdEncoding.EncodeToString([]byte("v")))
	badOpLine := fmt.Sprintf(`{"op":"upsert","key":%q,"value":%q}`+"\n",
		base64.StdEncoding.EncodeToString([]byte("k")),
		base64.StdEncoding.EncodeToString([]byte("v")))
	cases := []struct {
		name  string
		stdin string
	}{
		{"empty-key", emptyKeyLine},
		{"oversize-key", bigKeyLine},
		{"bad-op", badOpLine},
		{"bad-json", "{not json\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, code := invoke([]string{"batch", "--addr", badAddr, "-"}, tc.stdin)
			if code != exitUsage {
				t.Fatalf("code=%d want exitUsage=%d", code, exitUsage)
			}
		})
	}
}

// TestE2ERangeStartGreaterThanEnd pins the local check that catches
// reversed range bounds before dialing. Without it, the server would
// reject with *wire.ProtocolError and the CLI would report exit 1.
func TestE2ERangeStartGreaterThanEnd(t *testing.T) {
	const badAddr = "127.0.0.1:1"
	_, _, code := invoke([]string{
		"range", "--addr", badAddr, "--start", "z", "--end", "a",
	}, "")
	if code != exitUsage {
		t.Fatalf("code=%d want exitUsage=%d", code, exitUsage)
	}
}

// TestE2EBatchEncodedSizeExceedsFramePayload pins the local check that
// catches an aggregate batch larger than wire.MaxFramePayload. Without
// it, the CLI would dial, the server would reject with *wire.FrameError,
// and the user would see exit 1. We pack several entries that pass each
// per-entry check yet together overflow the 32 MiB frame budget.
func TestE2EBatchEncodedSizeExceedsFramePayload(t *testing.T) {
	const badAddr = "127.0.0.1:1"
	// 3 entries × ~12 MiB raw value = ~36 MiB body. wire.MaxFramePayload
	// is 32 MiB, so the local accumulator must trip on the 3rd entry.
	const rawSize = 12 * 1024 * 1024
	val := bytes.Repeat([]byte{'V'}, rawSize)
	v := base64.StdEncoding.EncodeToString(val)
	var stdin strings.Builder
	for i, k := range []string{"a", "b", "c"} {
		kB64 := base64.StdEncoding.EncodeToString([]byte(k))
		stdin.WriteString(fmt.Sprintf(`{"op":"put","key":%q,"value":%q}`+"\n", kB64, v))
		_ = i
	}
	_, errb, code := invoke([]string{"batch", "--addr", badAddr, "-"}, stdin.String())
	if code != exitUsage {
		t.Fatalf("code=%d stderr=%q want exitUsage=%d", code, errb, exitUsage)
	}
	if !strings.Contains(errb, "max frame payload") {
		t.Fatalf("stderr=%q (want frame-payload diagnostic)", errb)
	}
}
