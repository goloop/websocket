package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echoHandler upgrades and echoes every message back until the peer closes.
func echoHandler(opts ...Option) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ws, err := Upgrade(w, r, opts...)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			mt, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if err := ws.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dialEcho(t *testing.T, srv *httptest.Server, opts ...DialOption) *Conn {
	t.Helper()
	ws, resp, err := Dial(context.Background(), wsURL(srv), opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	return ws
}

func TestEchoTextAndBinary(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	cases := []struct {
		mt   MessageType
		body string
	}{
		{TextMessage, "hello, world"},
		{BinaryMessage, "\x00\x01\x02\xff"},
		{TextMessage, ""},
		{TextMessage, strings.Repeat("A", 200000)}, // exercises 64-bit length
	}
	for _, tc := range cases {
		if err := ws.WriteMessage(tc.mt, []byte(tc.body)); err != nil {
			t.Fatalf("write: %v", err)
		}
		mt, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if mt != tc.mt || string(data) != tc.body {
			t.Fatalf("echo mismatch: type %d len %d", mt, len(data))
		}
	}
}

func TestFragmentedMessageReassembled(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	// Send "Hel" + "lo" as two frames; the server must reassemble to "Hello".
	ws.writeMu.Lock()
	err1 := ws.writeFrameLocked(TextMessage, false, false, []byte("Hel"))
	err2 := ws.writeFrameLocked(continuationFrame, true, false, []byte("lo"))
	ws.writeMu.Unlock()
	if err1 != nil || err2 != nil {
		t.Fatalf("write frames: %v %v", err1, err2)
	}

	mt, data, err := ws.ReadMessage()
	if err != nil || mt != TextMessage || string(data) != "Hello" {
		t.Fatalf("reassembly: mt=%d data=%q err=%v", mt, data, err)
	}
}

func TestControlFrameBetweenFragments(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	// A ping arrives between two data fragments; the server must still
	// reassemble the message and answer the ping.
	ws.writeMu.Lock()
	ws.writeFrameLocked(TextMessage, false, false, []byte("Hel"))
	ws.writeFrameLocked(PingMessage, true, false, []byte("ka"))
	ws.writeFrameLocked(continuationFrame, true, false, []byte("lo"))
	ws.writeMu.Unlock()

	// The pong is consumed by ReadMessage internally; we get the text back.
	mt, data, err := ws.ReadMessage()
	if err != nil || mt != TextMessage || string(data) != "Hello" {
		t.Fatalf("mt=%d data=%q err=%v", mt, data, err)
	}
}

func TestPingAutoPong(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	got := make(chan string, 1)
	ws.SetPongHandler(func(appData string) error {
		got <- appData
		return nil
	})

	if err := ws.WriteControl(PingMessage, []byte("ping-data"), time.Now().Add(time.Second)); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	// Trigger reads so the pong is processed.
	go ws.ReadMessage()

	select {
	case appData := <-got:
		if appData != "ping-data" {
			t.Fatalf("pong payload = %q, want ping-data", appData)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive auto-pong")
	}
}

func TestCloseHandshake(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	if err := ws.CloseWithStatus(CloseNormalClosure, "bye"); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, _, err := ws.ReadMessage()
	if !IsCloseError(err, CloseNormalClosure) {
		t.Fatalf("expected close 1000, got %v", err)
	}
}

func TestReadLimit(t *testing.T) {
	srv := httptest.NewServer(echoHandler(WithReadLimit(16)))
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	if err := ws.WriteMessage(TextMessage, []byte(strings.Repeat("x", 100))); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, _, err := ws.ReadMessage()
	if !IsCloseError(err, CloseMessageTooBig) {
		t.Fatalf("expected close 1009, got %v", err)
	}
}

func TestSubprotocolNegotiation(t *testing.T) {
	srv := httptest.NewServer(echoHandler(WithSubprotocols("chat", "echo")))
	defer srv.Close()
	ws := dialEcho(t, srv, WithDialSubprotocols("superchat", "echo"))
	defer ws.Close()

	if ws.Subprotocol() != "echo" {
		t.Fatalf("subprotocol = %q, want echo", ws.Subprotocol())
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	srv := httptest.NewServer(echoHandler(WithCompression()))
	defer srv.Close()
	ws := dialEcho(t, srv, WithDialCompression())
	defer ws.Close()

	if !ws.writeCompression {
		t.Fatal("compression was not negotiated")
	}
	body := strings.Repeat("the quick brown fox ", 500)
	if err := ws.WriteMessage(TextMessage, []byte(body)); err != nil {
		t.Fatalf("write: %v", err)
	}
	mt, data, err := ws.ReadMessage()
	if err != nil || mt != TextMessage || string(data) != body {
		t.Fatalf("compressed echo mismatch: err=%v len=%d", err, len(data))
	}
}

func TestJSON(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	type payload struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}
	if err := ws.WriteJSON(payload{Name: "goloop", N: 42}); err != nil {
		t.Fatalf("write json: %v", err)
	}
	var got payload
	if err := ws.ReadJSON(&got); err != nil {
		t.Fatalf("read json: %v", err)
	}
	if got.Name != "goloop" || got.N != 42 {
		t.Fatalf("json mismatch: %+v", got)
	}
}

func TestOriginRejectedByDefault(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()

	h := http.Header{}
	h.Set("Origin", "http://evil.example.com")
	_, resp, err := Dial(context.Background(), wsURL(srv), WithDialHeader(h))
	if err == nil {
		t.Fatal("expected handshake to be rejected for cross-origin request")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got resp=%v err=%v", resp, err)
	}
}

func TestOriginAllowedWithChecker(t *testing.T) {
	srv := httptest.NewServer(echoHandler(
		WithOriginChecker(func(r *http.Request) bool { return true })))
	defer srv.Close()

	h := http.Header{}
	h.Set("Origin", "http://any.example.com")
	ws, resp, err := Dial(context.Background(), wsURL(srv), WithDialHeader(h))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	ws.Close()
}
