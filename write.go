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
		return ErrBadWriteType
	}
	if c.writeLimit > 0 && int64(len(data)) > c.writeLimit {
		return ErrWriteLimit
	}

	c.lockWrite()
	defer c.unlockWrite()
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
// the whole call, including the wait for a data write in progress to finish;
// when it passes first the frame is not sent and the error reports a timeout.
// A zero deadline means no timeout. WriteControl may be called from a
// goroutine other than the one writing messages.
func (c *Conn) WriteControl(mt MessageType, data []byte, deadline time.Time) error {
	// Exactly the three control types, not "any opcode with the high bit
	// set": the frame writer masks the opcode to four bits, so a number like
	// 24 used to be written as a close frame while the connection went on
	// believing it had sent nothing.
	if mt != CloseMessage && mt != PingMessage && mt != PongMessage {
		return ErrBadControl
	}
	if len(data) > maxControlFramePayload {
		return ErrControlTooBig
	}
	if mt == CloseMessage && !validClosePayload(data) {
		// Refused before the lock is taken, so a bad argument neither writes
		// bytes nor changes the state of the connection.
		return ErrBadClosePayload
	}

	if err := c.lockWriteUntil(deadline); err != nil {
		return err
	}
	defer c.unlockWrite()
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
		if err := c.beginControlDeadline(deadline); err != nil {
			return err
		}
		defer c.endControlDeadline()
	}
	if err := c.writeFrameLocked(mt, true, false, data); err != nil {
		return err
	}
	if mt == CloseMessage {
		c.closeSent = true
		c.closeTold.Store(true)
	}
	return nil
}

// NextWriter returns a writer for the next message of the given data type. The
// message is sent when the writer is closed. Only one writer may be open at a
// time.
func (c *Conn) NextWriter(mt MessageType) (io.WriteCloser, error) {
	if mt != TextMessage && mt != BinaryMessage {
		return nil, ErrBadWriteType
	}
	// Refuse now rather than at Close. Everything written to this writer is
	// buffered until then, so a connection that is already finished would
	// otherwise be told only after the caller had built the whole message,
	// which for a large one means spending the memory to learn nothing.
	if err := c.writeDead.Load(); err != nil {
		return nil, *err
	}
	if c.closeTold.Load() {
		return nil, ErrCloseSent
	}
	return &messageWriter{c: c, mt: mt}, nil
}

// messageWriter buffers a message and sends it as one frame on Close.
//
// It is not a streaming writer: nothing reaches the network until Close, so
// the whole message is held in memory first. [Conn.SetWriteLimit] bounds that.
// True streaming fragmentation can be added later without changing this API.
type messageWriter struct {
	c      *Conn
	mt     MessageType
	buf    bytes.Buffer
	closed bool
	err    error // the result of the one Close that did the work
}

// Write appends to the in-memory message buffer. It fails once the writer has
// been closed; nothing is sent to the peer until Close.
func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrWriteClosed
	}
	if limit := w.c.writeLimit; limit > 0 &&
		int64(w.buf.Len())+int64(len(p)) > limit {
		// Refuse before buffering: the point of the limit is to stop the
		// buffer growing, so accepting these bytes first would defeat it.
		return 0, ErrWriteLimit
	}
	return w.buf.Write(p)
}

// Close flushes the buffered bytes as a single message frame and marks the
// writer done. It is idempotent: a second call is a no-op and returns nil.
func (w *messageWriter) Close() error {
	if w.closed {
		// Answer the same thing every time. Returning nil to a second Close
		// after the first had failed reported success for a message that was
		// never sent, and a deferred Close is exactly where that is read.
		return w.err
	}
	w.closed = true

	c := w.c
	err := c.WriteMessage(w.mt, w.buf.Bytes())

	// Let the message go. An application that keeps a closed writer, which is
	// easy to do when one is stored in a struct, would otherwise hold the
	// whole buffer and the connection with it. Both methods above check
	// w.closed first, so nothing here is read again.
	w.buf = bytes.Buffer{}
	w.c = nil
	w.err = err

	return err
}
