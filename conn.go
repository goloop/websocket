package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// defaultControlDeadline bounds the automatic writes the connection performs on
// its own (auto-pong, close echo) so a stuck peer cannot block the reader.
const defaultControlDeadline = 10 * time.Second

// maxReadLimit is the largest message limit that keeps the derived compressed
// bound (limit + limit/8 + 64) a positive int64. It is far past any real
// message, so clamping to it costs nothing and removes the overflow.
const maxReadLimit = (math.MaxInt64 - 64) / 2

// Errors a caller can act on. They are values rather than messages so that a
// read or write loop can tell a message that was too big from a peer that
// broke the protocol from a call that was simply wrong, with errors.Is and
// without matching on text.
var (
	// ErrReadLimit means a message exceeded [Conn.SetReadLimit]. The
	// connection is closed with 1009.
	ErrReadLimit = errors.New("websocket: read limit exceeded")

	// ErrProtocol means the peer broke the framing protocol. Every such
	// violation matches it, and the connection is closed with 1002; the
	// error's own text says which rule was broken.
	ErrProtocol = errors.New("websocket: protocol error")

	// The rest are programming errors: the arguments of a call were wrong,
	// so nothing is written and the connection is left as it was.
	ErrWriteClosed     = errors.New("websocket: write to closed message writer")
	ErrBadControl      = errors.New("websocket: not a control frame type")
	ErrControlTooBig   = errors.New("websocket: control frame payload too large")
	ErrBadWriteType    = errors.New("websocket: not a data message type")
	ErrBadClosePayload = errors.New(
		"websocket: close payload has a reserved code or invalid reason")
)

var (
	errInvalidUTF8 = protocolError("invalid UTF-8 in text message")

	// Both EOF sentinels wrap io.ErrUnexpectedEOF so that a caller can match
	// "the connection ended in the middle of something" with one errors.Is,
	// whichever half it happened in.
	errUnexpectedEOF = fmt.Errorf(
		"websocket: unexpected EOF reading a frame: %w", io.ErrUnexpectedEOF)

	// errIncompleteMessage is the connection ending between fragments, while
	// the peer still owed a continuation frame. It is not the end of the
	// message: what arrived so far is a prefix, never a whole message.
	errIncompleteMessage = fmt.Errorf(
		"websocket: connection closed before the final fragment: %w",
		io.ErrUnexpectedEOF)
)

// protocolError is an error caused by a peer violating the framing protocol. It
// makes the connection send a 1002 close before failing the read.
type protocolError string

// Unwrap ties every framing violation to [ErrProtocol], so one errors.Is
// recognises them all without naming each rule.
func (e protocolError) Unwrap() error { return ErrProtocol }

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
	reader        *messageReader // the reader handed out for the message in flight
	inflated      []byte         // payload of the compressed message just returned
	inMessage     bool           // a message is currently being read across frames
	readDecomp    bool           // permessage-deflate negotiated for reading

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
	writeLimit       int64 // max size of one outgoing message, 0 for no bound

	// Write deadlines, guarded by their own mutex and never by the write
	// lock, so setting a deadline can interrupt a write in flight instead of
	// waiting behind it. Two deadlines are tracked: the one the user set and
	// the one an in-flight control write is bounded by. What reaches the
	// socket is the effective deadline of the two, so neither can silently
	// lift the other's bound.
	deadlineMu      sync.Mutex
	writeDeadline   time.Time // as set by SetWriteDeadline
	controlDeadline time.Time // of the control write in flight, if any
	controlActive   bool
	userSetDuring   bool // SetWriteDeadline was called during that write
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
	if c.controlActive {
		c.userSetDuring = true
	}
	return c.applyWriteDeadlineLocked()
}

// applyWriteDeadlineLocked puts the effective write deadline on the socket.
// The caller must hold deadlineMu.
//
// A control write given an explicit deadline is governed by it: it supersedes
// whatever deadline the connection already carried, which is what lets the
// reader answer a ping or send a close with a bound of its own even though the
// application set a short deadline for its data writes.
//
// A SetWriteDeadline call made while that control write is in flight may
// shorten its bound but never lift it. Shortening is always safe, since asking
// a write to end sooner cannot leave anything stuck; lengthening it, including
// clearing it, would break the promise the control call was given, and the
// reader depends on that promise to get past a peer that stopped reading.
func (c *Conn) applyWriteDeadlineLocked() error {
	effective := c.writeDeadline
	if c.controlActive && !c.controlDeadline.IsZero() {
		switch {
		case !c.userSetDuring, effective.IsZero():
			effective = c.controlDeadline
		case c.controlDeadline.Before(effective):
			effective = c.controlDeadline
		}
	}
	return c.conn.SetWriteDeadline(effective)
}

// beginControlDeadline bounds the control write that is about to start. A zero
// deadline leaves the user's own deadline in charge.
func (c *Conn) beginControlDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.controlDeadline = deadline
	c.controlActive = true
	c.userSetDuring = false
	return c.applyWriteDeadlineLocked()
}

// endControlDeadline drops the control write's bound and puts the user's own
// deadline back on the socket.
func (c *Conn) endControlDeadline() {
	c.deadlineMu.Lock()
	c.controlDeadline = time.Time{}
	c.controlActive = false
	c.userSetDuring = false
	_ = c.applyWriteDeadlineLocked()
	c.deadlineMu.Unlock()
}

// SetReadLimit sets the maximum size in bytes of a single received message.
// A message that would exceed it fails the read and closes the connection with
// 1009. A value <= 0 restores the default.
func (c *Conn) SetReadLimit(n int64) {
	if n <= 0 {
		n = defaultReadLimit
	}
	// The compressed form of a message is allowed a little more than the
	// limit, and that sum has to stay a positive number: an enormous limit
	// used to wrap around and reject everything, including a five-byte
	// message. Anything past this is not a limit anyone means.
	if n > maxReadLimit {
		n = maxReadLimit
	}
	c.readLimit = n
}

// SetWriteLimit sets the maximum size in bytes of a single outgoing message.
// A write past it fails with [ErrWriteLimit] and nothing is sent; the
// connection stays usable. A value <= 0, the default, means no bound.
//
// It exists because a message is built in memory before any of it goes out:
// [Conn.NextWriter] buffers everything written to it until Close, so
// io.Copy from an unbounded source is bounded by nothing but available
// memory. A limit turns that into an error the application can handle.
//
// Like SetReadLimit it is not synchronized with writes in progress: set it
// before the connection is handed to the code that writes.
func (c *Conn) SetWriteLimit(n int64) {
	if n < 0 {
		n = 0
	}
	c.writeLimit = n
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
	// A code of 0 means "no status" and sends an empty payload; anything else
	// has to be a code this end may put on the wire.
	if code != 0 && !isValidSentCloseCode(code) {
		return ErrBadClosePayload
	}
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
	err = asAbnormalClose(err)
	if c.readErr == nil {
		c.readErr = err
	}
	return err
}

// asAbnormalClose reports the connection ending without a closing handshake
// the way every other ending is reported: as a *CloseError. Without it, a peer
// that simply vanished produced a bare io.EOF, which IsUnexpectedCloseError
// does not recognise, so the one helper meant for telling a clean shutdown
// from a surprising one stayed silent for the most surprising case of all.
//
// The original error is kept as the cause, so matching io.EOF or
// io.ErrUnexpectedEOF with errors.Is keeps working.
func asAbnormalClose(err error) error {
	var ce *CloseError
	if errors.As(err, &ce) {
		return err // the peer said why
	}
	if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err // a fault of its own, not the connection ending
	}
	return &CloseError{Code: CloseAbnormalClosure, cause: err}
}
