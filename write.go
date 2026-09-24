package websocket

import (
	"bytes"
	"compress/flate"
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
	if c.msgInFlight {
		// A streamed message has frames on the wire already; this one's
		// frames would be read as continuations of it.
		return ErrMessageInFlight
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

// NextWriter returns a writer for the next message of the given data type.
//
// The message is streamed: once more than a fragment's worth of data has been
// written, it starts going out as fragments, so writing a message larger than
// memory is a matter of writing it, not of holding it. A message that fits in
// one fragment is still sent as a single unfragmented frame when the writer is
// closed, exactly as before.
//
// Only one message may be in flight at a time. From the moment the first
// fragment goes out until Close, another data write returns
// [ErrMessageInFlight]; control frames are unaffected and still go out between
// fragments, so pings and closes keep working while a long message is being
// sent. Close must be called to end the message.
func (c *Conn) NextWriter(mt MessageType) (io.WriteCloser, error) {
	if mt != TextMessage && mt != BinaryMessage {
		return nil, ErrBadWriteType
	}
	// Refuse now rather than at Close: a connection that is already finished
	// would otherwise be reported only after the caller had built the whole
	// message.
	if err := c.writeDead.Load(); err != nil {
		return nil, *err
	}
	if c.closeTold.Load() {
		return nil, ErrCloseSent
	}
	if c.inFlight.Load() {
		return nil, ErrMessageInFlight
	}

	w := &messageWriter{c: c, mt: mt}
	if c.writeCompression {
		// One deflate stream per message ("no context takeover"), written
		// into the pending buffer as the caller writes.
		w.fw = getFlateWriter(&w.buf, c.compressionLevel)
	}
	return w, nil
}

// messageWriter streams one message. Data written to it gathers in buf until
// there is a fragment's worth, which is then sent; the rest follows on Close.
//
// With compression, buf holds the deflate output rather than the caller's
// bytes, and the last four bytes are always held back: the sync-flush octets
// that end the stream have to be stripped from the end of the message, and
// they can only be recognised there.
type messageWriter struct {
	c       *Conn
	mt      MessageType
	buf     bytes.Buffer
	fw      *flate.Writer // non-nil when the message is compressed
	started bool          // a fragment has gone out, so this message is open
	closed  bool
	err     error // the result of the one Close that did the work
	n       int64 // bytes accepted from the caller, for the write limit
}

// Write adds bytes to the message, sending a fragment whenever enough have
// gathered. It fails once the writer has been closed.
func (w *messageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrWriteClosed
	}
	if limit := w.c.writeLimit; limit > 0 && w.n+int64(len(p)) > limit {
		// Refuse before buffering or sending: a message that cannot be
		// completed should not have part of it on the wire.
		return 0, ErrWriteLimit
	}

	var err error
	if w.fw != nil {
		_, err = w.fw.Write(p)
	} else {
		_, err = w.buf.Write(p)
	}
	if err != nil {
		return 0, err
	}
	w.n += int64(len(p))

	if err := w.flushFragments(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// reserve is how many pending bytes must not be sent yet. For a compressed
// message that is the four octets of the sync flush, which are stripped from
// the very end of the message and so can never be part of a fragment.
func (w *messageWriter) reserve() int {
	if w.fw != nil {
		return len(deflateSyncTail)
	}
	return 0
}

// flushFragments sends whole fragments while enough bytes have gathered.
func (w *messageWriter) flushFragments() error {
	size := w.c.fragmentSize
	if size <= 0 {
		size = defaultFragmentSize
	}
	for w.buf.Len()-w.reserve() >= size {
		if err := w.sendFragment(size, false); err != nil {
			return err
		}
	}
	return nil
}

// sendFragment writes n pending bytes as one frame. The first frame of a
// message carries the message type, and the compression bit when the message
// is compressed; every later one is a continuation.
func (w *messageWriter) sendFragment(n int, fin bool) error {
	c := w.c

	// Next hands back the buffer's own bytes rather than a copy. They stay
	// valid until the buffer is next written to, and nothing writes to it
	// between here and the frame going out, while writeFrameLocked copies
	// what it needs. That saves a copy of every fragment of every message.
	payload := w.buf.Next(n)

	opcode, compressed := w.mt, w.fw != nil
	if w.started {
		opcode, compressed = continuationFrame, false
	}

	c.lockWrite()
	defer c.unlockWrite()
	if c.writeErr != nil {
		return c.writeErr
	}
	if c.closeSent {
		return ErrCloseSent
	}
	if !w.started && c.msgInFlight {
		return ErrMessageInFlight
	}

	if err := c.writeFrameLocked(opcode, fin, compressed, payload); err != nil {
		w.setInFlightLocked(false)
		return err
	}
	w.started = true
	w.setInFlightLocked(!fin)
	return nil
}

// setInFlightLocked records whether this message still owes the peer a final
// frame. The caller must hold the write lock.
func (w *messageWriter) setInFlightLocked(open bool) {
	w.c.msgInFlight = open
	w.c.inFlight.Store(open)
}

// Close sends what is left as the final frame and ends the message. It is
// idempotent, and every call returns the result of the one that did the work.
func (w *messageWriter) Close() error {
	if w.closed {
		// Returning nil to a second Close after the first had failed reported
		// success for a message that never went out, and a deferred Close is
		// exactly where that is read.
		return w.err
	}
	w.closed = true
	w.err = w.finish()

	// Let the message go. An application that keeps a closed writer, which is
	// easy to do when one is stored in a struct, would otherwise hold the
	// whole buffer and the connection with it. Both methods above check
	// w.closed first, so nothing here is read again.
	w.buf = bytes.Buffer{}
	w.fw = nil
	w.c = nil

	return w.err
}

// finish flushes the compressor, strips the sync-flush tail and sends the
// remainder as the final frame.
func (w *messageWriter) finish() error {
	if w.fw != nil {
		err := w.fw.Flush()
		putFlateWriter(w.fw, w.c.compressionLevel)
		if err != nil {
			w.abandon()
			return err
		}
		// RFC 7692: the sender removes the four octets the sync flush ends
		// with from the end of the message payload.
		if b := w.buf.Bytes(); len(b) >= len(deflateSyncTail) &&
			bytes.Equal(b[len(b)-len(deflateSyncTail):], deflateSyncTail) {
			w.buf.Truncate(len(b) - len(deflateSyncTail))
		}
	}
	return w.sendFragment(w.buf.Len(), true)
}

// abandon gives up on a message whose fragments are already on the wire. The
// peer is owed a final frame that will never come, so the connection cannot
// be used for data again; the error recorded by the failed write is what
// every later write reports.
func (w *messageWriter) abandon() {
	if !w.started {
		return
	}
	c := w.c
	c.lockWrite()
	w.setInFlightLocked(false)
	_ = c.setWriteErr(io.ErrUnexpectedEOF)
	c.unlockWrite()
}
