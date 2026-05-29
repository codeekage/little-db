// Package client is the Go client for the little-db TCP server. It is a
// thin wrapper over internal/wire: one TCP connection, one method per
// wire opcode, transport policy (deadlines, dial timeout) layered on
// top. The CLI in cmd/little-db uses it; tests can use it directly to
// drive a server end-to-end.
//
// Concurrency:
//
//   - A *Client is NOT safe for concurrent use. The wire protocol has no
//     in-flight pipelining: serialising calls on one connection is the
//     caller's responsibility. To issue requests concurrently, dial
//     multiple Clients. This matches the server's per-connection serial
//     dispatch model and keeps the client implementation lock-free.
//
// Error model:
//
//   - Transport errors (dial failure, EOF, deadline) surface as the
//     underlying net.OpError / io.EOF / os.ErrDeadlineExceeded. Use
//     errors.Is / errors.As to inspect.
//
//   - Protocol errors (the server sent something this client can't
//     parse) surface as *wire.FrameError or *wire.ProtocolError. These
//     indicate a version skew or bug; the client cannot recover.
//
//   - Server-side errors (the request was framed and parsed, but the
//     server returned a non-OK status) surface as *wire.RemoteError
//     EXCEPT for GET-not-found, which surfaces as ErrNotFound. This
//     mirrors the engine's API surface: NOT_FOUND on GET is a normal
//     control-flow signal and gets a sentinel; everything else is a
//     real error the caller probably wants to inspect.
package client

import (
	"errors"
	"fmt"
	"net"
	"time"

	"little-db/internal/wire"
)

// Default dial/request timeouts. Chosen to be obviously not-zero without
// being adversarial: 5 s to connect to a server on localhost is
// comfortably loose; 30 s for one request matches the server's default
// ReadDeadline + WriteDeadline so a single in-flight call won't time
// out before the server's own deadline would.
const (
	DefaultDialTimeout    = 5 * time.Second
	DefaultRequestTimeout = 30 * time.Second
)

// ErrNotFound is the sentinel returned by Get when the key does not
// exist. It is NOT returned for any other operation; Delete on a missing
// key is a successful no-op per the engine contract.
var ErrNotFound = errors.New("client: key not found")

// Options configures Dial.
type Options struct {
	// DialTimeout caps how long Dial may block waiting for the TCP
	// handshake. Zero means use DefaultDialTimeout.
	DialTimeout time.Duration

	// RequestTimeout is applied to every request as the read AND write
	// deadline. For READKEYRANGE it is re-applied per page (so a long
	// stream is allowed, but any single page must arrive within
	// RequestTimeout of the previous one). Zero means use
	// DefaultRequestTimeout.
	RequestTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultDialTimeout
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = DefaultRequestTimeout
	}
	return o
}

// Client is a single TCP connection to a little-db server. Not safe for
// concurrent use.
type Client struct {
	conn net.Conn
	opts Options
}

// Dial opens a connection to addr and returns a ready Client. The
// returned Client owns the underlying net.Conn — call Close exactly once.
func Dial(addr string, opts Options) (*Client, error) {
	opts = opts.withDefaults()
	conn, err := net.DialTimeout("tcp", addr, opts.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("client: dial %q: %w", addr, err)
	}
	return &Client{conn: conn, opts: opts}, nil
}

// NewClient wraps an already-connected net.Conn. Mostly useful for
// tests using net.Pipe; production callers want Dial.
func NewClient(conn net.Conn, opts Options) *Client {
	return &Client{conn: conn, opts: opts.withDefaults()}
}

// Close closes the underlying connection. Subsequent calls are no-ops.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// LocalAddr returns the local end of the connection (mostly useful for
// tests and logging).
func (c *Client) LocalAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

// RemoteAddr returns the server's address.
func (c *Client) RemoteAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

// setDeadlines applies RequestTimeout to both read and write directions.
// Called immediately before each request so a slow previous call doesn't
// shorten the budget for this one.
func (c *Client) setDeadlines() error {
	if c.conn == nil {
		return errors.New("client: closed")
	}
	d := time.Now().Add(c.opts.RequestTimeout)
	return c.conn.SetDeadline(d)
}

// clearDeadlines removes any deadline. Used between calls so an idle
// client does not surprise the next operation with an expired deadline.
func (c *Client) clearDeadlines() {
	if c.conn != nil {
		_ = c.conn.SetDeadline(time.Time{})
	}
}

// roundTripUnary writes one request and reads exactly one response
// frame. Returns the response status, body, or an error.
func (c *Client) roundTripUnary(req wire.Request) (wire.Status, []byte, error) {
	if err := c.setDeadlines(); err != nil {
		return 0, nil, err
	}
	defer c.clearDeadlines()
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		return 0, nil, err
	}
	if _, err := c.conn.Write(frame); err != nil {
		return 0, nil, err
	}
	tag, respBody, err := wire.ReadFrame(c.conn)
	if err != nil {
		return 0, nil, err
	}
	return wire.Status(tag), respBody, nil
}

// statusToErr maps a non-OK status into the right error shape.
// status==OK returns nil.
func statusToErr(status wire.Status, body []byte) error {
	if status == wire.StatusOK {
		return nil
	}
	msg, derr := wire.DecodeError(body)
	if derr != nil {
		return derr
	}
	return &wire.RemoteError{Status: status, Msg: msg}
}

// Put writes (key, value). Returns nil on success or a *wire.RemoteError
// for any server-side failure.
func (c *Client) Put(key, value []byte) error {
	status, body, err := c.roundTripUnary(&wire.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	return statusToErr(status, body)
}

// Get returns the value for key. Returns ErrNotFound if the key is not
// present, *wire.RemoteError for any other server-side failure.
func (c *Client) Get(key []byte) ([]byte, error) {
	status, body, err := c.roundTripUnary(&wire.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	if status == wire.StatusNotFound {
		return nil, ErrNotFound
	}
	if status != wire.StatusOK {
		return nil, statusToErr(status, body)
	}
	return wire.DecodeGetOK(body)
}

// Delete removes key. Missing keys are a successful no-op (matches the
// engine's idempotent-delete contract).
func (c *Client) Delete(key []byte) error {
	status, body, err := c.roundTripUnary(&wire.DeleteRequest{Key: key})
	if err != nil {
		return err
	}
	return statusToErr(status, body)
}

// Batch applies entries atomically.
func (c *Client) Batch(entries []wire.BatchEntry) error {
	status, body, err := c.roundTripUnary(&wire.BatchRequest{Entries: entries})
	if err != nil {
		return err
	}
	return statusToErr(status, body)
}

// Ping is a liveness check. Returns nil if the server replied OK.
func (c *Client) Ping() error {
	status, body, err := c.roundTripUnary(&wire.PingRequest{})
	if err != nil {
		return err
	}
	return statusToErr(status, body)
}

// Stats returns the engine's live key count and on-disk byte total.
func (c *Client) Stats() (keyCount, bytesOnDisk uint64, err error) {
	status, body, rerr := c.roundTripUnary(&wire.StatsRequest{})
	if rerr != nil {
		return 0, 0, rerr
	}
	if status != wire.StatusOK {
		return 0, 0, statusToErr(status, body)
	}
	return wire.DecodeStatsOK(body)
}

// Promote asks a follower to become a writable leader. Returns nil on
// OK; a non-follower replies BAD_REQUEST ("not a follower") which
// surfaces as *wire.RemoteError. The call is one PROMOTE frame with
// no body.
func (c *Client) Promote() error {
	status, body, err := c.roundTripUnary(&wire.PromoteRequest{})
	if err != nil {
		return err
	}
	return statusToErr(status, body)
}

// ReadKeyRange streams (key, value) pairs in [start, end). Either bound
// may be nil for open-ended. For each page, pageFn is invoked with the
// decoded pairs; if pageFn returns false, the stream is abandoned and
// ReadKeyRange returns wire.ErrStreamStopped. The connection is then
// unusable for further requests because the server may still be
// streaming; the caller MUST Close.
//
// Per-page deadline: RequestTimeout is re-applied before each ReadFrame
// so a server stuck mid-stream surfaces a deadline error within
// RequestTimeout of the last page. The total stream is otherwise
// unbounded.
func (c *Client) ReadKeyRange(start, end []byte, pageFn func(pairs []wire.KV) bool) error {
	if pageFn == nil {
		return errors.New("client: ReadKeyRange pageFn must not be nil")
	}
	// Write the request under the same deadline shape as a unary call.
	if err := c.setDeadlines(); err != nil {
		return err
	}
	req := &wire.ReadKeyRangeRequest{Start: start, End: end}
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		c.clearDeadlines()
		return err
	}
	if _, err := c.conn.Write(frame); err != nil {
		c.clearDeadlines()
		return err
	}
	// Stream: re-arm the read deadline per frame so a long stream
	// works as long as inter-frame latency stays under RequestTimeout.
	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.RequestTimeout)); err != nil {
			c.clearDeadlines()
			return err
		}
		tag, fbody, err := wire.ReadFrame(c.conn)
		if err != nil {
			c.clearDeadlines()
			return err
		}
		status := wire.Status(tag)
		if status != wire.StatusOK {
			c.clearDeadlines()
			msg, derr := wire.DecodeError(fbody)
			if derr != nil {
				return derr
			}
			return &wire.RemoteError{Status: status, Msg: msg}
		}
		pairs, end, err := wire.DecodeRangeFrame(fbody)
		if err != nil {
			c.clearDeadlines()
			return err
		}
		if end {
			c.clearDeadlines()
			return nil
		}
		if !pageFn(pairs) {
			// Caller asked us to stop. The server is still
			// streaming; the only safe move is to close. Do NOT
			// clear deadlines on a connection we're about to
			// declare unusable.
			return wire.ErrStreamStopped
		}
	}
}
