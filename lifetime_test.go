package websocket

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"
)

// A reader belongs to one message. Once the connection has moved on, reading
// from the old one must say so rather than quietly serving bytes of the next
// message to code that thinks it is still reading the first.
func TestStaleReaderIsRefused(t *testing.T) {
	wire := append(
		buildFrame(true, false, BinaryMessage, false, []byte("first")),
		buildFrame(true, false, BinaryMessage, false, []byte("second"))...,
	)
	c := newFuzzConn(wire)

	_, first, err := c.NextReader()
	if err != nil {
		t.Fatalf("NextReader: %v", err)
	}
	if _, err := first.Read(make([]byte, 2)); err != nil {
		t.Fatalf("read from the first message: %v", err)
	}

	if _, _, err := c.NextReader(); err != nil {
		t.Fatalf("second NextReader: %v", err)
	}
	if n, err := first.Read(make([]byte, 8)); !errors.Is(err, ErrStaleReader) {
		t.Errorf("stale read returned n=%d err=%v, want ErrStaleReader", n, err)
	}
}

// A second Close answers what the first one did, so a deferred Close after a
// failed one does not report success for a message that never went out.
func TestRepeatedCloseKeepsTheResult(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)

	w, err := ws.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write([]byte("never arrives")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Break the connection under the writer, so the flush on Close fails.
	ws.NetConn().Close()

	firstErr := w.Close()
	if firstErr == nil {
		t.Skip("the write succeeded against a closed connection; nothing to check")
	}
	if secondErr := w.Close(); !errors.Is(secondErr, firstErr) {
		t.Errorf("second Close = %v, want the first result %v", secondErr, firstErr)
	}
}

// A writer is refused up front on a connection that is already finished,
// instead of accepting a whole message and failing at Close.
func TestNextWriterRefusesAFinishedConnection(t *testing.T) {
	t.Run("after a close was sent", func(t *testing.T) {
		c := newConn(nopConn{}, false, nil, "", false, 0)
		if err := c.CloseWithStatus(CloseNormalClosure, ""); err != nil {
			t.Fatalf("CloseWithStatus: %v", err)
		}
		if _, err := c.NextWriter(TextMessage); !errors.Is(err, ErrCloseSent) {
			t.Errorf("NextWriter = %v, want ErrCloseSent", err)
		}
	})

	t.Run("after a write failed", func(t *testing.T) {
		srv := httptest.NewServer(echoHandler())
		defer srv.Close()
		ws := dialEcho(t, srv)
		ws.NetConn().Close()

		// The first write records the sticky failure.
		if err := ws.WriteMessage(TextMessage, []byte("x")); err == nil {
			t.Skip("the write succeeded against a closed connection")
		}

		w, err := ws.NextWriter(TextMessage)
		if err == nil {
			t.Fatal("NextWriter handed out a writer for a dead connection")
		}
		if w != nil {
			t.Error("a refused NextWriter returned a writer")
		}
	})
}

// The early refusal must not cost the ordinary path: a healthy connection
// still hands out a writer, and a big message still goes through.
func TestNextWriterStillWorksOnALiveConnection(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	w, err := ws.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	body := bytes.Repeat([]byte("payload"), 1024)
	if _, err := io.Copy(w, bytes.NewReader(body)); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, got, err := ws.ReadMessage()
	if err != nil || mt != BinaryMessage || !bytes.Equal(got, body) {
		t.Fatalf("mt=%d len=%d err=%v, want the message back", mt, len(got), err)
	}
}
