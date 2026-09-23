package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// A message whose last frame is not final is never a whole message: the
// connection ending there is io.ErrUnexpectedEOF, not the end of the message.
// The prefix that did arrive must not reach the caller as a complete message,
// and the error must be sticky so a later read cannot succeed either.
func TestIncompleteFragmentIsNotAMessage(t *testing.T) {
	t.Run("ReadMessage", func(t *testing.T) {
		c := newFuzzConn(buildFrame(false, false, BinaryMessage, false, []byte("partial")))
		mt, body, err := c.ReadMessage()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v (type=%d body=%q), want io.ErrUnexpectedEOF", err, mt, body)
		}
		if _, _, err := c.ReadMessage(); err == nil {
			t.Error("a read after a truncated message succeeded")
		}
	})

	t.Run("NextReader and ReadAll", func(t *testing.T) {
		c := newFuzzConn(buildFrame(false, false, TextMessage, false, []byte("par")))
		_, r, err := c.NextReader()
		if err != nil {
			t.Fatalf("NextReader: %v", err)
		}
		body, err := io.ReadAll(r)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadAll err = %v (body=%q), want io.ErrUnexpectedEOF", err, body)
		}
	})

	t.Run("after a complete continuation", func(t *testing.T) {
		// Two non-final fragments, then EOF: still owed a final frame.
		wire := append(
			buildFrame(false, false, BinaryMessage, false, []byte("one")),
			buildFrame(false, false, continuationFrame, false, []byte("two"))...,
		)
		c := newFuzzConn(wire)
		if _, _, err := c.ReadMessage(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("while draining", func(t *testing.T) {
		// Read one byte, then ask for the next message: the drain runs into
		// the same truncated fragmentation and must report it.
		c := newFuzzConn(buildFrame(false, false, BinaryMessage, false, []byte("partial")))
		_, r, err := c.NextReader()
		if err != nil {
			t.Fatalf("NextReader: %v", err)
		}
		if _, err := r.Read(make([]byte, 1)); err != nil {
			t.Fatalf("Read: %v", err)
		}
		if _, _, err := c.NextReader(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("NextReader err = %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("compressed", func(t *testing.T) {
		var buf bytes.Buffer
		fw, _ := flate.NewWriter(&buf, flate.DefaultCompression)
		fw.Write(bytes.Repeat([]byte("payload "), 64))
		fw.Flush()
		body := buf.Bytes()

		c := newFuzzConn(buildFrame(false, true, BinaryMessage, false, body))
		if _, _, err := c.ReadMessage(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("a final frame still ends the message", func(t *testing.T) {
		wire := append(
			buildFrame(false, false, BinaryMessage, false, []byte("one")),
			buildFrame(true, false, continuationFrame, false, []byte("two"))...,
		)
		c := newFuzzConn(wire)
		mt, body, err := c.ReadMessage()
		if err != nil || mt != BinaryMessage || string(body) != "onetwo" {
			t.Fatalf("mt=%d body=%q err=%v, want the reassembled message", mt, body, err)
		}
	})
}

// A control write given an explicit deadline is bounded by it even when
// another goroutine clears the connection's write deadline while it is in
// flight: clearing must not lift a bound the control call was promised.
func TestControlDeadlineSurvivesConcurrentClear(t *testing.T) {
	srv, peer := net.Pipe()
	defer srv.Close()
	defer peer.Close()
	c := newConn(srv, true, nil, "", false, 0)

	done := make(chan error, 1)
	go func() {
		done <- c.WriteControl(PingMessage, []byte("x"), time.Now().Add(30*time.Millisecond))
	}()

	// Let the control write reach the blocked pipe, then clear the deadline.
	time.Sleep(15 * time.Millisecond)
	if err := c.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the control write reported success to a peer that never read")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the control write outlived its deadline after a concurrent clear")
	}
}

// Shortening the deadline while a control write is in flight still takes
// effect: asking a write to end sooner is always allowed.
func TestControlDeadlineCanBeShortened(t *testing.T) {
	srv, peer := net.Pipe()
	defer srv.Close()
	defer peer.Close()
	c := newConn(srv, true, nil, "", false, 0)

	done := make(chan error, 1)
	go func() {
		done <- c.WriteControl(PingMessage, []byte("x"), time.Now().Add(10*time.Second))
	}()

	time.Sleep(15 * time.Millisecond)
	if err := c.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the control write reported success to a peer that never read")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shortening the deadline did not reach the write in flight")
	}
}
