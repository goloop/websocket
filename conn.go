package websocket

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"
)

// defaultControlDeadline bounds the automatic writes the connection performs on
// its own (auto-pong, close echo) so a stuck peer cannot block the reader.
const defaultControlDeadline = 10 * time.Second

var (
	errWriteClosed   = errors.New("websocket: write to closed message writer")
	errReadLimit     = errors.New("websocket: read limit exceeded")
	errBadControl    = errors.New("websocket: not a control frame type")
	errControlTooBig = errors.New("websocket: control frame payload too large")
	errBadWriteType  = errors.New("websocket: not a data message type")
	errUnexpectedEOF = errors.New("websocket: unexpected EOF reading a frame")
	errInvalidUTF8   = protocolError("invalid UTF-8 in text message")
)

// protocolError is an error caused by a peer violating the framing protocol. It
// makes the connection send a 1002 close before failing the read.
type protocolError string

// Error implements the error interface, prefixing the violation with
// "websocket: protocol error: ".
func (e protocolError) Error() string { return "websocket: protocol error: " + string(e) }

// Conn is a WebSocket connection. It carries a single logical stream of
// messages in each direction over a hijacked net.Conn.
//
// A Conn supports one concurrent reader and one concurrent writer; see the
// package documentation. WriteControl may be called concurrently with a writer.
type Conn struct {
	conn        net.Conn
	br          *bufio.Reader
	isServer    bool
	subprotocol string

	// Read state, owned by the single reader goroutine.
	readErr       error
	readRemaining int64 // unread bytes in the current frame payload
	readFinal     bool  // FIN flag of the current frame
	readMasked    bool
	readMaskKey   [4]byte
	readMaskPos   int
	readLimit     int64
	readLength    int64 // bytes delivered for the current message (for the limit)
	readMsgType   MessageType
	inMessage     bool // a message is currently being read across frames
	readDecomp    bool // permessage-deflate negotiated for reading

	// Handlers for received control frames.
	pingHandler  func(string) error
	pongHandler  func(string) error
	closeHandler func(CloseCode, string) error

	// Write state. The write lock is a one-slot channel rather than a mutex
	// so a control write can give up waiting for it: a data write that is
	// stuck on a peer that stopped reading must not also hold hostage the
	// close, ping and pong frames the reader needs to send.
	writeLock        chan struct{} // holds one token while a write is in progress
	writeErr         error         // guarded by writeLock
	closeSent        bool          // guarded by writeLock
	writeCompression bool
	compressionLevel int

	// The user's write deadline, guarded by its own mutex and never by the
	// write lock, so setting a deadline can interrupt a write in flight
	// instead of waiting behind it.
	deadlineMu    sync.Mutex
	writeDeadline time.Time
}

// errWriteTimeout is returned by WriteControl when its deadline passes while a
// write is still in progress. It reports itself as a timeout so callers can
// treat it like a deadline on the connection.
var errWriteTimeout = writeTimeoutError{}

type writeTimeoutError struct{}

// Error implements the error interface.
func (writeTimeoutError) Error() string { return "websocket: write timeout" }

// Timeout reports true so the error satisfies net.Error's timeout check.
func (writeTimeoutError) Timeout() bool { return true }

// Temporary reports true: a later control write may succeed once the write in
// progress finishes.
func (writeTimeoutError) Temporary() bool { return true }

// newConn builds a Conn around an already-hijacked connection. br may be a
// buffered reader that already holds bytes read during the handshake.
func newConn(conn net.Conn, isServer bool, br *bufio.Reader, subprotocol string, compression bool, level int) *Conn {
	if br == nil {
		br = bufio.NewReader(conn)
	}
	c := &Conn{
		conn:             conn,
		br:               br,
		isServer:         isServer,
		subprotocol:      subprotocol,
		readLimit:        defaultReadLimit,
		readDecomp:       compression,
		writeLock:        make(chan struct{}, 1),
		writeCompression: compression,
		compressionLevel: level,
	}
	return c
}

// lockWrite takes the write lock, waiting as long as it takes.
func (c *Conn) lockWrite() { c.writeLock <- struct{}{} }

// lockWriteUntil takes the write lock, giving up with errWriteTimeout once
// the deadline passes. A zero deadline waits as long as it takes.
func (c *Conn) lockWriteUntil(deadline time.Time) error {
	if deadline.IsZero() {
		c.lockWrite()
		return nil
	}
	select {
	case c.writeLock <- struct{}{}:
		return nil
	default:
	}
	wait := time.Until(deadline)
	if wait <= 0 {
		return errWriteTimeout
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case c.writeLock <- struct{}{}:
		return nil
	case <-timer.C:
		return errWriteTimeout
	}
}

// unlockWrite releases the write lock.
func (c *Conn) unlockWrite() { <-c.writeLock }

// Subprotocol returns the negotiated subprotocol, or an empty string if none was
// selected.
func (c *Conn) Subprotocol() string { return c.subprotocol }

// NetConn returns the underlying network connection. Reading from or writing to
// it directly will corrupt the WebSocket stream; it is an escape hatch for
// deadlines, addresses and similar low-level needs.
func (c *Conn) NetConn() net.Conn { return c.conn }

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// SetReadDeadline sets the deadline for future reads. A zero value clears it.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for future writes and for a write already
// in progress, as on a net.Conn. A zero value clears it. The deadline is
// remembered so that the connection's own control writes (auto-pong, close
// echo) can restore it instead of leaving a stale deadline.
//
// It does not wait for the write lock: a write that is stuck on a peer that
// stopped reading is exactly what a deadline is for.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.writeDeadline = t
	return c.conn.SetWriteDeadline(t)
}

// restoreWriteDeadline puts the user's deadline back on the connection after a
// control write set a temporary one. The store and the call are made under the
// same lock so a concurrent SetWriteDeadline can never be undone by a stale
// value.
func (c *Conn) restoreWriteDeadline() {
	c.deadlineMu.Lock()
	_ = c.conn.SetWriteDeadline(c.writeDeadline)
	c.deadlineMu.Unlock()
}

// SetReadLimit sets the maximum size in bytes of a single received message.
// A message that would exceed it fails the read and closes the connection with
// 1009. A value <= 0 restores the default.
func (c *Conn) SetReadLimit(n int64) {
	if n <= 0 {
		n = defaultReadLimit
	}
	c.readLimit = n
}

// SetPingHandler sets the handler for received ping frames. The default sends a
// pong with the same payload. Setting a handler disables the automatic pong, so
// a custom handler that wants to reply must do so itself.
func (c *Conn) SetPingHandler(fn func(appData string) error) { c.pingHandler = fn }

// SetPongHandler sets the handler for received pong frames. The default ignores
// them.
func (c *Conn) SetPongHandler(fn func(appData string) error) { c.pongHandler = fn }

// SetCloseHandler sets the handler for received close frames. The default sends
// a close frame back. A custom handler replaces that behaviour.
func (c *Conn) SetCloseHandler(fn func(code CloseCode, text string) error) { c.closeHandler = fn }

// Close closes the underlying network connection without performing the closing
// handshake. For a graceful shutdown, call CloseWithStatus first, then Close.
func (c *Conn) Close() error { return c.conn.Close() }

// CloseWithStatus sends a close frame with the given code and reason. It does
// not close the network connection; the peer is expected to answer with its own
// close, after which the reader returns a *CloseError and the caller should call
// Close. A code of 0 sends an empty close payload.
func (c *Conn) CloseWithStatus(code CloseCode, reason string) error {
	return c.WriteControl(CloseMessage, formatCloseMessage(code, reason),
		time.Now().Add(defaultControlDeadline))
}

// setWriteErr records a permanent write error and returns it.
func (c *Conn) setWriteErr(err error) error {
	if c.writeErr == nil {
		c.writeErr = err
	}
	return c.writeErr
}

// abort turns a protocol violation into a 1002 close and returns the error to
// the reader. Non-protocol errors (I/O, EOF) pass through unchanged.
func (c *Conn) abort(err error) error {
	var pe protocolError
	if errors.As(err, &pe) {
		_ = c.WriteControl(CloseMessage,
			formatCloseMessage(CloseProtocolError, ""),
			time.Now().Add(defaultControlDeadline))
	}
	if c.readErr == nil {
		c.readErr = err
	}
	return err
}
