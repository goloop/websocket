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
)

// protocolError is an error caused by a peer violating the framing protocol. It
// makes the connection send a 1002 close before failing the read.
type protocolError string

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

	// Write state.
	writeMu          sync.Mutex // guards all writes to conn
	writeErr         error
	closeSent        bool
	writeCompression bool
	compressionLevel int
}

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
		writeCompression: compression,
		compressionLevel: level,
	}
	return c
}

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

// SetWriteDeadline sets the deadline for future writes. A zero value clears it.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

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
