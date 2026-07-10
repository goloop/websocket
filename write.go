package websocket

import (
	"bytes"
	"io"
	"time"
)

// WriteMessage sends data as a single, unfragmented message of the given type.
// If write compression is enabled, a data message is compressed with
// permessage-deflate. For control types it is equivalent to WriteControl with
// the write deadline already set on the connection.
func (c *Conn) WriteMessage(mt MessageType, data []byte) error {
	if isControl(mt) {
		return c.WriteControl(mt, data, time.Time{})
	}
	if mt != TextMessage && mt != BinaryMessage {
		return errBadWriteType
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	if c.closeSent {
		return ErrCloseSent
	}

	payload, compressed := data, false
	if c.writeCompression {
		out, err := deflate(data, c.compressionLevel)
		if err != nil {
			return err
		}
		payload, compressed = out, true
	}
	return c.writeFrameLocked(mt, true, compressed, payload)
}

// WriteControl sends a control frame (close, ping or pong). The deadline bounds
// the write; a zero deadline means no timeout. WriteControl may be called from a
// goroutine other than the one writing messages.
func (c *Conn) WriteControl(mt MessageType, data []byte, deadline time.Time) error {
	if !isControl(mt) {
		return errBadControl
	}
	if len(data) > maxControlFramePayload {
		return errControlTooBig
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	if c.closeSent {
		return ErrCloseSent
	}

	// A zero deadline means "use the deadline already on the connection", so
	// only touch it when the caller asked for a specific one, and restore the
	// user's deadline afterwards so an internal control write (auto-pong, close
	// echo) never leaves a stale deadline that would kill later writes.
	if !deadline.IsZero() {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
		defer func() { _ = c.conn.SetWriteDeadline(c.writeDeadline) }()
	}
	if err := c.writeFrameLocked(mt, true, false, data); err != nil {
		return err
	}
	if mt == CloseMessage {
		c.closeSent = true
	}
	return nil
}

// NextWriter returns a writer for the next message of the given data type. The
// message is sent when the writer is closed. Only one writer may be open at a
// time.
func (c *Conn) NextWriter(mt MessageType) (io.WriteCloser, error) {
	if mt != TextMessage && mt != BinaryMessage {
		return nil, errBadWriteType
	}
	return &messageWriter{c: c, mt: mt}, nil
}

// messageWriter buffers a message and sends it as one frame on Close. It is a
// simple, correct writer for v0; true streaming fragmentation can be added later
// without changing this API.
type messageWriter struct {
	c      *Conn
	mt     MessageType
	buf    bytes.Buffer
	closed bool
}

// Write appends to the in-memory message buffer. It fails once the writer has
// been closed; nothing is sent to the peer until Close.
func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errWriteClosed
	}
	return w.buf.Write(p)
}

// Close flushes the buffered bytes as a single message frame and marks the
// writer done. It is idempotent: a second call is a no-op and returns nil.
func (w *messageWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.c.WriteMessage(w.mt, w.buf.Bytes())
}
