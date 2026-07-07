package websocket

import (
	"compress/flate"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNextWriterAndReader(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	w, err := ws.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("next writer: %v", err)
	}
	w.Write([]byte("hel"))
	w.Write([]byte("lo"))
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("write after close should fail")
	}

	mt, r, err := ws.NextReader()
	if err != nil {
		t.Fatalf("next reader: %v", err)
	}
	data, _ := io.ReadAll(r)
	if mt != TextMessage || string(data) != "hello" {
		t.Fatalf("streamed message = %q", data)
	}
}

func TestNextReaderDrainsPrevious(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	if err := ws.WriteMessage(TextMessage, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteMessage(TextMessage, []byte("second")); err != nil {
		t.Fatal(err)
	}

	// Open the first message but do not read it.
	if _, _, err := ws.NextReader(); err != nil {
		t.Fatalf("first next reader: %v", err)
	}
	// The next call must drain the first and deliver the second.
	_, r, err := ws.NextReader()
	if err != nil {
		t.Fatalf("second next reader: %v", err)
	}
	if data, _ := io.ReadAll(r); string(data) != "second" {
		t.Fatalf("after drain got %q, want second", data)
	}
}

func TestPingHandlerCustom(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, PingMessage, false, []byte("hi")))
	got := ""
	c.SetPingHandler(func(s string) error { got = s; return nil })
	c.ReadMessage() // processes the ping, then reaches EOF
	if got != "hi" {
		t.Fatalf("ping handler saw %q", got)
	}
}

func TestPongHandlerCustom(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, PongMessage, false, []byte("po")))
	got := ""
	c.SetPongHandler(func(s string) error { got = s; return nil })
	c.ReadMessage()
	if got != "po" {
		t.Fatalf("pong handler saw %q", got)
	}
}

func TestCloseHandlerCustom(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, CloseMessage, false, []byte{0x03, 0xe8})) // 1000
	var code CloseCode
	c.SetCloseHandler(func(cc CloseCode, txt string) error { code = cc; return nil })
	_, _, err := c.ReadMessage()
	if !IsCloseError(err, CloseNormalClosure) || code != CloseNormalClosure {
		t.Fatalf("close handler code=%d err=%v", code, err)
	}
}

func TestCloseErrorString(t *testing.T) {
	e := &CloseError{Code: CloseGoingAway, Text: "away"}
	if got := e.Error(); got == "" || got[:10] != "websocket:" {
		t.Fatalf("unexpected error string %q", got)
	}
	e2 := &CloseError{Code: CloseNormalClosure}
	_ = e2.Error()
}

func TestConnAccessors(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	if ws.NetConn() == nil {
		t.Fatal("NetConn nil")
	}
	if ws.LocalAddr() == nil || ws.RemoteAddr() == nil {
		t.Fatal("addr nil")
	}
	ws.SetReadLimit(0) // restores default
	if ws.readLimit != defaultReadLimit {
		t.Fatal("read limit not restored")
	}
	ws.SetReadLimit(1024)
	_ = ws.SetReadDeadline(time.Now().Add(time.Minute))
	_ = ws.SetWriteDeadline(time.Now().Add(time.Minute))
}

func TestHandshakeErrorString(t *testing.T) {
	if (HandshakeError{message: "x"}).Error() != "x" {
		t.Fatal("handshake error string")
	}
}

func TestOptionSettersCompile(t *testing.T) {
	// Construct with every option so the setters are exercised.
	_ = NewUpgrader(
		WithSubprotocols("a", "b"),
		WithReadLimit(1<<20),
		WithCompression(),
		WithCompressionLevel(flate.BestSpeed),
		WithHandshakeTimeout(time.Second),
		WithOriginChecker(func(*http.Request) bool { return true }),
	)

	cfg := newDialConfig()
	for _, opt := range []DialOption{
		WithDialHeader(http.Header{"X": {"y"}}),
		WithDialSubprotocols("a"),
		WithDialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		WithDialNetDialer(&net.Dialer{Timeout: time.Second}),
		WithDialNetDialer(nil), // ignored
		WithDialCompression(),
	} {
		opt(cfg)
	}
	if cfg.tlsConfig == nil || !cfg.compression {
		t.Fatal("dial options not applied")
	}

	// tlsClientConfig defaults the server name.
	if tlsClientConfig(nil, "example.com").ServerName != "example.com" {
		t.Fatal("server name not defaulted")
	}
}

func TestDialBadScheme(t *testing.T) {
	if _, _, err := Dial(nil, "http://example.com"); err == nil { //nolint:staticcheck
		t.Fatal("expected error for non-ws scheme")
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	const n = 200
	done := make(chan struct{})

	// Reader goroutine.
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Writer goroutine, plus concurrent control frames.
	for i := 0; i < n; i++ {
		if err := ws.WriteMessage(BinaryMessage, []byte("payload")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if i%20 == 0 {
			_ = ws.WriteControl(PingMessage, []byte("p"), time.Now().Add(time.Second))
		}
	}
	<-done
}
