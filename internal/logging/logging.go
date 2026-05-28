// Package logging is the little-db wrapper around log/slog. It exists
// for two reasons:
//
//   - To give engine and server a single shared "nil logger" sentinel
//     (Nop) so passing nil into Options is safe at every call site
//     without nil-guards in the hot path.
//   - To centralise the --log-level / --log-format flag parsing the
//     CLI uses to construct a handler, so future surfaces (tests,
//     embedded uses) get the same vocabulary.
//
// The package is intentionally tiny — slog already covers structured
// logging well; this is just glue.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// Format selects the on-wire handler. Text is the default for terminals;
// JSON is the default for production / log shippers.
type Format int

const (
	FormatText Format = iota
	FormatJSON
)

// ParseFormat accepts "text" or "json" (case-insensitive).
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(s) {
	case "", "text":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	default:
		return FormatText, fmt.Errorf("unknown log format %q (want text|json)", s)
	}
}

// ParseLevel accepts "debug", "info", "warn", "error" (case-insensitive).
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level %q (want debug|info|warn|error)", s)
	}
}

// New constructs a slog.Logger writing to w at the given level/format.
// w defaults to os.Stderr at the call site (we don't import os here so
// the package stays trivially testable).
func New(w io.Writer, level slog.Level, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch format {
	case FormatJSON:
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}

// Nop returns a process-wide shared logger that discards everything.
// Its Enabled method always returns false, so hot-path callers that
// guard expensive attribute construction with Logger.Enabled pay
// effectively nothing when logging is off.
func Nop() *slog.Logger {
	nopOnce.Do(func() { nopLogger = slog.New(nopHandler{}) })
	return nopLogger
}

var (
	nopOnce   sync.Once
	nopLogger *slog.Logger
)

// nopHandler is a zero-allocation slog.Handler that drops every record.
type nopHandler struct{}

func (nopHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (nopHandler) Handle(context.Context, slog.Record) error { return nil }
func (nopHandler) WithAttrs([]slog.Attr) slog.Handler        { return nopHandler{} }
func (nopHandler) WithGroup(string) slog.Handler             { return nopHandler{} }
