package websocket

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// countingHandler echoes messages and records how many frames each one
// arrived in, so a test can tell a streamed message from a buffered one.
func streamEchoServer(t *testing.T, opts ...Option) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := Upgrade(w, r, opts...)
		if err != nil {
			return
		}
		defer ws.Close()
		ws.SetReadLimit(64 << 20)
		for {
			mt, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if err := ws.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A message larger than the fragment size streams out in fragments and comes
// back whole: fragmentation must be invisible to the peer.
func TestStreamedMessageRoundTrips(t *testing.T) {
	for _, compress := range []bool{false, true} {
		name := "plain"
		var sopts []Option
		var dopts []DialOption
		if compress {
			name = "compressed"
			sopts = append(sopts, WithCompression())
			dopts = append(dopts, WithDialCompression())
		}
		t.Run(name, func(t *testing.T) {
			srv := streamEchoServer(t, sopts...)
			ws, _, err := Dial(context.Background(), "ws"+srv.URL[4:], dopts...)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer ws.Close()
			ws.SetReadLimit(64 << 20)
			ws.fragmentSize = 4096 // fragment aggressively

			body := make([]byte, 300*1024)
			if _, err := rand.Read(body); err != nil {
				t.Fatal(err)
			}

			w, err := ws.NextWriter(BinaryMessage)
			if err != nil {
				t.Fatalf("NextWriter: %v", err)
			}
			if _, err := io.Copy(w, bytes.NewReader(body)); err != nil {
				t.Fatalf("Copy: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
			mt, got, err := ws.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if mt != BinaryMessage || !bytes.Equal(got, body) {
				t.Fatalf("mt=%d len=%d want len=%d and equal bytes", mt, len(got), len(body))
			}
		})
	}
}

// The writer must not hold the whole message: once past a fragment, bytes are
// on the wire and the buffer stays near the fragment size.
func TestStreamedWriterDoesNotHoldTheMessage(t *testing.T) {
	srv := streamEchoServer(t)
	ws, _, err := Dial(context.Background(), "ws"+srv.URL[4:])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.Close()
	ws.fragmentSize = 4096

	w, err := ws.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	mw := w.(*messageWriter)
	for range 64 {
		if _, err := w.Write(make([]byte, 4096)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if mw.buf.Len() >= 4096 {
		t.Errorf("writer holds %d pending bytes, want less than one fragment", mw.buf.Len())
	}
	if !mw.started {
		t.Error("nothing went out while 256 KiB was written")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// A message that fits in one fragment is still one unfragmented frame, so the
// wire is unchanged for the ordinary case.
func TestSmallMessageStaysOneFrame(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, 0)
	rec := &frameRecorder{}
	c.conn = rec

	w, err := c.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write([]byte("small")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if rec.writes != 1 {
		t.Errorf("message went out in %d frames, want 1", rec.writes)
	}
	if c.inFlight.Load() {
		t.Error("the connection still thinks a message is open")
	}
}

// Control frames go out between fragments: a long message must not silence
// pings and closes for its whole duration.
func TestControlFramesFlowBetweenFragments(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, 0)
	c.fragmentSize = 64

	w, err := c.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(make([]byte, 256)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !c.inFlight.Load() {
		t.Fatal("no fragment went out")
	}

	if err := c.WriteControl(PingMessage, []byte("alive"), time.Time{}); err != nil {
		t.Errorf("ping between fragments: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// Another data message cannot start while one is in flight: its frames would
// be read as continuations of the open one.
func TestSecondMessageRefusedWhileInFlight(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, 0)
	c.fragmentSize = 64

	w, err := c.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(make([]byte, 256)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := c.WriteMessage(TextMessage, []byte("cut in")); !errors.Is(err, ErrMessageInFlight) {
		t.Errorf("WriteMessage = %v, want ErrMessageInFlight", err)
	}
	if _, err := c.NextWriter(TextMessage); !errors.Is(err, ErrMessageInFlight) {
		t.Errorf("NextWriter = %v, want ErrMessageInFlight", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Once the message is closed the connection is free again.
	if err := c.WriteMessage(TextMessage, []byte("now fine")); err != nil {
		t.Errorf("write after the streamed message: %v", err)
	}
}

// The write limit covers the whole message, fragments included, and refuses
// before any part of an oversized message reaches the wire.
func TestWriteLimitAppliesAcrossFragments(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, 0)
	c.fragmentSize = 64
	c.SetWriteLimit(200)

	w, err := c.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(make([]byte, 128)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := w.Write(make([]byte, 128)); !errors.Is(err, ErrWriteLimit) {
		t.Errorf("second write = %v, want ErrWriteLimit", err)
	}
	_ = w.Close()
}

// frameRecorder counts the frames written to it.
type frameRecorder struct {
	nopConn
	writes int
	buf    bytes.Buffer
}

func (r *frameRecorder) Write(p []byte) (int, error) {
	r.writes++
	return r.buf.Write(p)
}

// The streamed writer must put the same bytes on the wire as WriteMessage for
// a message that fits in one frame, compressed or not: the new path is only
// supposed to add fragmentation, not to change what a small message looks
// like.
func TestStreamedWriterMatchesWriteMessage(t *testing.T) {
	for _, compress := range []bool{false, true} {
		name := "plain"
		if compress {
			name = "compressed"
		}
		t.Run(name, func(t *testing.T) {
			for _, body := range [][]byte{
				nil,
				[]byte("hello"),
				bytes.Repeat([]byte("compressible "), 128),
			} {
				// A server connection does not mask, so the comparison is
				// of the frames themselves rather than of two random keys.
				direct := &frameRecorder{}
				c1 := newConn(direct, true, nil, "", compress, flate.DefaultCompression)
				if err := c1.WriteMessage(BinaryMessage, body); err != nil {
					t.Fatalf("WriteMessage: %v", err)
				}

				streamed := &frameRecorder{}
				c2 := newConn(streamed, true, nil, "", compress, flate.DefaultCompression)
				w, err := c2.NextWriter(BinaryMessage)
				if err != nil {
					t.Fatalf("NextWriter: %v", err)
				}
				if len(body) > 0 {
					if _, err := w.Write(body); err != nil {
						t.Fatalf("Write: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				if !bytes.Equal(direct.buf.Bytes(), streamed.buf.Bytes()) {
					t.Errorf("len(body)=%d: streamed wire differs\n direct  = %x\n streamed= %x",
						len(body), direct.buf.Bytes(), streamed.buf.Bytes())
				}
				if streamed.writes != 1 {
					t.Errorf("len(body)=%d went out in %d frames, want 1", len(body), streamed.writes)
				}
			}
		})
	}
}

// A compressible message larger than the fragment size still round-trips: the
// sync-flush tail has to be stripped from the very end of the message, not
// from the end of some fragment.
func TestStreamedCompressibleMessageRoundTrips(t *testing.T) {
	srv := streamEchoServer(t, WithCompression())
	ws, _, err := Dial(context.Background(), "ws"+srv.URL[4:], WithDialCompression())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.Close()
	ws.SetReadLimit(64 << 20)
	ws.fragmentSize = 1024

	body := bytes.Repeat([]byte("the same line over and over\n"), 20000)
	w, err := ws.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	mt, got, err := ws.ReadMessage()
	if err != nil || mt != TextMessage || !bytes.Equal(got, body) {
		t.Fatalf("mt=%d len=%d err=%v, want the message back intact", mt, len(got), err)
	}
}
