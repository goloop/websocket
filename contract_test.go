package websocket

import (
	"crypto/tls"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The upgrade failure is a *HandshakeError, which is what the reference
// promised and what errors.As with a pointer finds. It carries the status
// that was written, so a caller can classify without reading the message.
func TestHandshakeErrorIsAPointerWithStatus(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)

	_, err := Upgrade(w, r)
	var he *HandshakeError
	if !errors.As(err, &he) {
		t.Fatalf("errors.As(&*HandshakeError) failed for %#v", err)
	}
	if he.Status != http.StatusMethodNotAllowed {
		t.Errorf("Status = %d, want %d", he.Status, http.StatusMethodNotAllowed)
	}
}

// A zero Upgrader, and a nil origin checker, both mean the default policy
// rather than a panic on the first valid request.
func TestZeroUpgraderDoesNotPanic(t *testing.T) {
	for _, u := range []*Upgrader{{}, NewUpgrader(WithOriginChecker(nil))} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Connection", "Upgrade")
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Sec-WebSocket-Version", "13")
		r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")

		// The recorder cannot be hijacked, so the upgrade fails - but with an
		// error, not a panic, and not because of the origin.
		_, err := u.Upgrade(w, r)
		if err == nil {
			t.Fatal("expected the hijack to fail on a recorder")
		}
		var he *HandshakeError
		if errors.As(err, &he) && he.Status == http.StatusForbidden {
			t.Error("a zero Upgrader rejected a same-origin request")
		}
	}
}

// An enormous read limit must not wrap around into rejecting everything.
func TestHugeReadLimitStillAcceptsMessages(t *testing.T) {
	c := newFuzzConn(buildFrame(true, true, BinaryMessage, false, deflateFor(t, []byte("hi"))))
	c.SetReadLimit(math.MaxInt64)

	mt, body, err := c.ReadMessage()
	if err != nil || mt != BinaryMessage || string(body) != "hi" {
		t.Fatalf("mt=%d body=%q err=%v, want a small compressed message through", mt, body, err)
	}
}

// deflateFor compresses data the way a peer would for permessage-deflate.
func deflateFor(t *testing.T, data []byte) []byte {
	t.Helper()
	out, err := deflate(data, 1)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The writer refuses control frames it cannot represent, and refuses them
// before anything reaches the wire or the connection's state.
func TestWriteControlRejectsBadArguments(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, 0)

	for _, mt := range []MessageType{MessageType(11), MessageType(24), TextMessage} {
		if err := c.WriteControl(mt, nil, time.Time{}); !errors.Is(err, ErrBadControl) {
			t.Errorf("WriteControl(%d) = %v, want ErrBadControl", mt, err)
		}
	}
	if c.closeSent {
		t.Error("a refused control write marked the close as sent")
	}

	// Reserved codes describe a local observation and must never go out.
	for _, code := range []CloseCode{CloseNoStatusReceived, CloseAbnormalClosure, CloseTLSHandshake} {
		if err := c.CloseWithStatus(code, ""); !errors.Is(err, ErrBadClosePayload) {
			t.Errorf("CloseWithStatus(%d) = %v, want ErrBadClosePayload", code, err)
		}
	}
	// A reason that is not UTF-8 is not a reason.
	if err := c.WriteControl(CloseMessage, []byte{0x03, 0xe8, 0xff}, time.Time{}); !errors.Is(err, ErrBadClosePayload) {
		t.Errorf("close with invalid UTF-8 reason = %v, want ErrBadClosePayload", err)
	}
	if c.closeSent {
		t.Error("a refused close marked the close as sent")
	}

	// A valid close still works.
	if err := c.CloseWithStatus(CloseNormalClosure, "bye"); err != nil {
		t.Errorf("a valid close: %v", err)
	}
	if !c.closeSent {
		t.Error("a valid close did not mark the close as sent")
	}
}

// 1014 is a registered code, so a peer may send it.
func TestCloseCode1014IsAccepted(t *testing.T) {
	if !isValidReceivedCloseCode(CloseBadGateway) {
		t.Error("1014 Bad Gateway was refused")
	}
	if isValidReceivedCloseCode(CloseTLSHandshake) {
		t.Error("1015 must not appear on the wire")
	}
}

// The default origin policy accepts a same-origin request and refuses the
// shapes a host-only comparison used to let through.
func TestSameOriginPolicy(t *testing.T) {
	cases := []struct {
		name    string
		origins []string
		host    string
		tls     bool
		want    bool
	}{
		{"no origin", nil, "example.com", false, true},
		{"same host", []string{"http://example.com"}, "example.com", false, true},
		{"same host and port", []string{"http://example.com:8080"}, "example.com:8080", false, true},
		{"https origin on a plain listener", []string{"https://example.com"}, "example.com", false, true},
		{"other host", []string{"http://evil.com"}, "example.com", false, false},
		{"two origins", []string{"http://example.com", "http://evil.com"}, "example.com", false, false},
		{"opaque origin", []string{"null"}, "example.com", false, false},
		{"origin with a path", []string{"https://example.com/path"}, "example.com", false, false},
		{"foreign scheme", []string{"ftp://example.com"}, "example.com", false, false},
		{"different ports", []string{"http://example.com:1"}, "example.com:2", false, false},
		{"http origin on a TLS listener", []string{"http://example.com"}, "example.com", true, false},
		{"https origin on a TLS listener", []string{"https://example.com"}, "example.com", true, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Host = c.host
			for _, o := range c.origins {
				r.Header.Add("Origin", o)
			}
			if c.tls {
				r.TLS = &tls.ConnectionState{}
			} else {
				r.TLS = nil
			}
			if got := checkSameOrigin(r); got != c.want {
				t.Errorf("checkSameOrigin() = %v, want %v", got, c.want)
			}
		})
	}
}

// The client refuses a handshake that selects an extension it never offered,
// or one with parameters it cannot honour.
func TestServerExtensionsAreValidated(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		offered bool
		wantErr bool
		wantOn  bool
	}{
		{"nothing selected", "", false, false, false},
		{"unrequested extension", "x-unrequested", false, true, false},
		{"unrequested extension while offering deflate", "x-unrequested", true, true, false},
		{"deflate never offered", "permessage-deflate; server_no_context_takeover", false, true, false},
		{"accepted", "permessage-deflate; server_no_context_takeover", true, false, true},
		{"flag carrying a value", "permessage-deflate; server_no_context_takeover=bad", true, true, false},
		{"repeated flag", "permessage-deflate; server_no_context_takeover; server_no_context_takeover", true, true, false},
		{"missing server_no_context_takeover", "permessage-deflate; client_no_context_takeover", true, true, false},
		{"window bits", "permessage-deflate; server_no_context_takeover; server_max_window_bits=10", true, true, false},
		{"trailing unknown extension", "permessage-deflate; server_no_context_takeover, x-unknown", true, true, false},
		{"twice", "permessage-deflate; server_no_context_takeover, permessage-deflate; server_no_context_takeover", true, true, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := http.Header{}
			if c.value != "" {
				h.Set("Sec-WebSocket-Extensions", c.value)
			}
			on, err := validateServerExtensions(h, c.offered)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
			if on != c.wantOn {
				t.Errorf("compression = %v, want %v", on, c.wantOn)
			}
		})
	}
}

// Headers a handler set before Upgrade must reach the client, cookies above
// all, and a value that could split the response must not.
func TestUpgradeCarriesPreparedHeaders(t *testing.T) {
	t.Run("carried", func(t *testing.T) {
		got := upgradeResponse(t, func(w http.ResponseWriter) {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc"})
			w.Header().Set("X-Trace", "t-1")
			w.Header().Add("Set-Cookie", "second=2")
			w.Header().Set("Sec-WebSocket-Accept", "forged")
		})
		for _, want := range []string{"session=abc", "second=2", "X-Trace: t-1"} {
			if !strings.Contains(got, want) {
				t.Errorf("response does not carry %q:\n%s", want, got)
			}
		}
		if strings.Contains(got, "forged") {
			t.Error("a handler overrode a protocol header")
		}
		if n := strings.Count(got, "Sec-WebSocket-Accept:"); n != 1 {
			t.Errorf("Sec-WebSocket-Accept appears %d times", n)
		}
	})

	t.Run("refused", func(t *testing.T) {
		got := upgradeResponse(t, func(w http.ResponseWriter) {
			w.Header()["X-Bad"] = []string{"a\r\nInjected: yes"}
		})
		// Go's own header writer turns CR and LF into spaces, so the text
		// can survive; what must not exist is a header line of its own.
		if strings.Contains(got, "\r\nInjected:") {
			t.Errorf("a split response was written:\n%s", got)
		}
		if !strings.Contains(got, "500") {
			t.Errorf("want the upgrade to fail, got:\n%s", got)
		}
	})
}

// upgradeResponse runs one upgrade against a real server and returns the raw
// bytes the client received, so the response can be read as the wire sees it.
func upgradeResponse(t *testing.T, prepare func(http.ResponseWriter)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prepare(w)
		ws, err := Upgrade(w, r)
		if err == nil {
			t.Cleanup(func() { ws.Close() })
		}
	}))
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := "GET / HTTP/1.1\r\nHost: " + srv.Listener.Addr().String() + "\r\n" +
		"Connection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	return string(buf[:n])
}
