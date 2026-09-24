package websocket

import (
	"compress/flate"
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// An option given a value this package cannot use fails the connection with
// ErrConfig, rather than being replaced by a default that hides the mistake.
func TestBadOptionsAreReported(t *testing.T) {
	t.Run("server", func(t *testing.T) {
		cases := map[string]Option{
			"compression level": WithCompressionLevel(42),
			"subprotocol token": WithSubprotocols("not a token"),
			"empty subprotocol": WithSubprotocols(""),
		}
		for name, opt := range cases {
			t.Run(name, func(t *testing.T) {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("Connection", "Upgrade")
				r.Header.Set("Upgrade", "websocket")
				r.Header.Set("Sec-WebSocket-Version", "13")
				r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")

				_, err := Upgrade(w, r, opt)
				var he *HandshakeError
				if !errors.As(err, &he) || he.Status != http.StatusInternalServerError {
					t.Fatalf("err = %v, want a 500 HandshakeError", err)
				}
			})
		}
	})

	t.Run("client", func(t *testing.T) {
		cases := map[string]DialOption{
			"compression level": WithDialCompressionLevel(42),
			"subprotocol token": WithDialSubprotocols("not a token"),
		}
		for name, opt := range cases {
			t.Run(name, func(t *testing.T) {
				_, _, err := Dial(context.Background(), "ws://127.0.0.1:1", opt)
				if !errors.Is(err, ErrConfig) {
					t.Errorf("err = %v, want ErrConfig", err)
				}
			})
		}
	})
}

// Configuration the caller still owns must not be shared: changing the slice
// or header afterwards cannot reach into a configured Upgrader or dial.
func TestOptionsCopyWhatTheCallerOwns(t *testing.T) {
	names := []string{"chat"}
	u := NewUpgrader(WithSubprotocols(names...))
	names[0] = "hijacked"
	if u.subprotocols[0] != "chat" {
		t.Errorf("subprotocols = %v, want the value at the time of the call", u.subprotocols)
	}

	h := http.Header{"X-Token": {"one"}}
	cfg := newDialConfig()
	WithDialHeader(h)(cfg)
	h.Set("X-Token", "two")
	if cfg.header.Get("X-Token") != "one" {
		t.Errorf("header = %q, want the value at the time of the call", cfg.header.Get("X-Token"))
	}

	tc := &tls.Config{ServerName: "one"}
	WithDialTLSConfig(tc)(cfg)
	tc.ServerName = "two"
	if cfg.tlsConfig.ServerName != "one" {
		t.Errorf("ServerName = %q, want the value at the time of the call", cfg.tlsConfig.ServerName)
	}
}

// A browser that offers client_max_window_bits must still get compression:
// declining the whole offer over a parameter this server may ignore meant no
// compression at all with the clients that send it.
func TestServerAcceptsWindowBitsOffer(t *testing.T) {
	cases := map[string]struct {
		offer string
		want  bool
	}{
		"bare deflate":              {"permessage-deflate", true},
		"client_max_window_bits":    {"permessage-deflate; client_max_window_bits", true},
		"client_max_window_bits=10": {"permessage-deflate; client_max_window_bits=10", true},
		"with no context takeover":  {"permessage-deflate; client_max_window_bits; client_no_context_takeover", true},
		"out of range bits":         {"permessage-deflate; client_max_window_bits=99", false},
		"server_max_window_bits":    {"permessage-deflate; server_max_window_bits=10", false},
		"repeated parameter":        {"permessage-deflate; client_no_context_takeover; client_no_context_takeover", false},
		"flag with a value":         {"permessage-deflate; client_no_context_takeover=1", false},
		"unknown parameter":         {"permessage-deflate; x-unknown", false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := http.Header{}
			h.Set("Sec-WebSocket-Extensions", c.offer)
			if got := serverAcceptsDeflateOffer(h); got != c.want {
				t.Errorf("serverAcceptsDeflateOffer(%q) = %v, want %v", c.offer, got, c.want)
			}
		})
	}
}

// The client can set what the server could already set, and can ask what was
// actually negotiated rather than what it hoped for.
func TestDialSymmetryAndCompressionAccessor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := Upgrade(w, r, WithCompression())
		if err != nil {
			return
		}
		defer ws.Close()
		// The plain dial below reaches this handler too, so the server's own
		// view is only asserted when the client actually offered compression.
		offered := r.Header.Get("Sec-WebSocket-Extensions") != ""
		if ws.CompressionEnabled() != offered {
			t.Errorf("server CompressionEnabled() = %v, offered = %v",
				ws.CompressionEnabled(), offered)
		}
		mt, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.WriteMessage(mt, data)
	}))
	defer srv.Close()

	ws, _, err := Dial(context.Background(), "ws"+srv.URL[4:],
		WithDialCompression(),
		WithDialCompressionLevel(flate.BestSpeed),
		WithDialReadLimit(4096))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.Close()

	if !ws.CompressionEnabled() {
		t.Error("compression was negotiated but CompressionEnabled reports false")
	}
	if ws.readLimit != 4096 {
		t.Errorf("readLimit = %d, want the dialled 4096", ws.readLimit)
	}
	if err := ws.WriteMessage(TextMessage, []byte("round trip")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, data, err := ws.ReadMessage(); err != nil || string(data) != "round trip" {
		t.Fatalf("data=%q err=%v", data, err)
	}

	// A plain connection reports no compression.
	plain, _, err := Dial(context.Background(), "ws"+srv.URL[4:])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer plain.Close()
	if plain.CompressionEnabled() {
		t.Error("a connection without compression reports it as enabled")
	}
}
