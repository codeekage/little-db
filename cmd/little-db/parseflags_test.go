package main

import (
	"flag"
	"io"
	"strings"
	"testing"
)

// newTestFlagSet builds a FlagSet that mirrors the subset of flags
// every ping/get/put-style subcommand registers, without dragging in
// any subcommand wiring. parseFlags is generic over the FlagSet so
// the exact flag list doesn't matter — only the parse behaviour does.
func newTestFlagSet() (*flag.FlagSet, *string, *string, *bool) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", "127.0.0.1:4242", "server address")
	dialTimeout := fs.String("dial-timeout", "2s", "dial timeout")
	plain := fs.Bool("plain", false, "treat key/value as raw UTF-8")
	return fs, addr, dialTimeout, plain
}

func TestParseFlagsHappyPath(t *testing.T) {
	fs, addr, _, plain := newTestFlagSet()
	if err := parseFlags(fs, []string{"--addr", "1.2.3.4:9000", "--plain", "k"}); err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if *addr != "1.2.3.4:9000" {
		t.Fatalf("addr: got %q want 1.2.3.4:9000", *addr)
	}
	if !*plain {
		t.Fatalf("plain: got false, want true")
	}
	if got := fs.Args(); len(got) != 1 || got[0] != "k" {
		t.Fatalf("positional: got %v want [k]", got)
	}
}

func TestParseFlagsInterspersedFlagsAndPositionals(t *testing.T) {
	fs, addr, _, plain := newTestFlagSet()
	// Mix flag in the middle of positionals — stdlib would stop parsing
	// at the first positional; parseFlags reorders so both flags land.
	if err := parseFlags(fs, []string{"k", "--addr", "x:1", "v", "--plain"}); err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if *addr != "x:1" {
		t.Fatalf("addr: got %q", *addr)
	}
	if !*plain {
		t.Fatalf("plain false")
	}
	want := []string{"k", "v"}
	got := fs.Args()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("positional: got %v want %v", got, want)
	}
}

// TestParseFlagsMissingValueAtEnd asserts the existing fix for a
// value-expecting flag with nothing after it produces the canonical
// stdlib error rather than absorbing a "--" sentinel.
func TestParseFlagsMissingValueAtEnd(t *testing.T) {
	fs, _, _, _ := newTestFlagSet()
	err := parseFlags(fs, []string{"--addr"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "needs an argument") {
		t.Fatalf("expected stdlib 'needs an argument' error, got: %v", err)
	}
}

// TestParseFlagsRefusesToConsumeAdjacentFlag is the regression for the
// post-v0.1.0 review finding: `--addr --dial-timeout 1ms` used to
// silently set addr="--dial-timeout" and leave 1ms as a positional.
// The right behaviour is the canonical stdlib "flag needs an argument:
// -addr" so the operator sees what they actually did wrong.
func TestParseFlagsRefusesToConsumeAdjacentFlag(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"long-form follow-on", []string{"--addr", "--dial-timeout", "1ms"}},
		{"single-dash follow-on", []string{"-addr", "-dial-timeout", "1ms"}},
		{"adjacent bool flag", []string{"--addr", "--plain"}},
		{"=value follow-on", []string{"--addr", "--dial-timeout=1ms"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, _, _ := newTestFlagSet()
			err := parseFlags(fs, tc.args)
			if err == nil {
				t.Fatalf("expected error, got nil; addr would have been silently corrupted")
			}
			if !strings.Contains(err.Error(), "needs an argument") {
				t.Fatalf("expected stdlib 'needs an argument' error, got: %v", err)
			}
			if !strings.Contains(err.Error(), "addr") {
				t.Fatalf("expected error to name -addr (the actually-missing flag), got: %v", err)
			}
		})
	}
}

// TestParseFlagsValueLooksLikeDashIsAccepted guards against the new
// check being too aggressive: a positional starting with `-` that is
// not a known flag must still be passed through as a value.
func TestParseFlagsValueLooksLikeDashIsAccepted(t *testing.T) {
	fs, addr, _, _ := newTestFlagSet()
	// "-unknown-host" is not a registered flag — it must be treated as
	// the value of --addr, not bounced as a missing-value error.
	if err := parseFlags(fs, []string{"--addr", "-unknown-host"}); err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if *addr != "-unknown-host" {
		t.Fatalf("addr: got %q want -unknown-host", *addr)
	}
}

// TestParseFlagsBareDashIsPositional preserves the conventional stdin
// marker behaviour: a bare "-" is not a flag and not a value-stealer.
func TestParseFlagsBareDashIsPositional(t *testing.T) {
	fs, _, _, _ := newTestFlagSet()
	if err := parseFlags(fs, []string{"--plain", "-"}); err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got := fs.Args(); len(got) != 1 || got[0] != "-" {
		t.Fatalf("positional: got %v want [-]", got)
	}
}
