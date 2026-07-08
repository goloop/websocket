package websocket

import (
	"compress/flate"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HandshakeError describes a failed WebSocket upgrade. When Upgrade returns one,
// it has already written an HTTP error response to the client.
type HandshakeError struct{ message string }

func (e HandshakeError) Error() string { return e.message }

// Upgrader holds a reusable server-side upgrade configuration.
type Upgrader struct {
	originChecker    func(*http.Request) bool
	subprotocols     []string
	readLimit        int64
	compression      bool
	compressionLevel int
	handshakeTimeout time.Duration
}

// NewUpgrader returns an Upgrader configured with the given options. By default
// it accepts only same-origin requests and does not enable compression.
func NewUpgrader(opts ...Option) *Upgrader {
	u := &Upgrader{
		originChecker:    checkSameOrigin,
		readLimit:        defaultReadLimit,
		compressionLevel: flate.DefaultCompression,
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// Upgrade is a convenience wrapper that upgrades r with a one-off configuration.
func Upgrade(w http.ResponseWriter, r *http.Request, opts ...Option) (*Conn, error) {
	return NewUpgrader(opts...).Upgrade(w, r)
}

// Upgrade completes the WebSocket handshake and returns a Conn. On failure it
// writes an HTTP error response and returns a *HandshakeError (or the hijack
// error).
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if r.Method != http.MethodGet {
		return u.fail(w, http.StatusMethodNotAllowed, "websocket: request method is not GET")
	}
	if !IsWebSocketUpgrade(r) {
		return u.fail(w, http.StatusBadRequest, "websocket: not a websocket upgrade request")
	}
	if !tokenListContainsValue(r.Header, "Sec-WebSocket-Version", "13") {
		w.Header().Set("Sec-WebSocket-Version", "13")
		return u.fail(w, http.StatusUpgradeRequired, "websocket: unsupported version, need 13")
	}
	challengeKey := r.Header.Get("Sec-WebSocket-Key")
	if challengeKey == "" {
		return u.fail(w, http.StatusBadRequest, "websocket: missing or empty Sec-WebSocket-Key")
	}
	if b, err := base64.StdEncoding.DecodeString(challengeKey); err != nil || len(b) != 16 {
		return u.fail(w, http.StatusBadRequest, "websocket: invalid Sec-WebSocket-Key")
	}
	if !u.originChecker(r) {
		return u.fail(w, http.StatusForbidden, "websocket: origin not allowed")
	}

	subprotocol := u.selectSubprotocol(r)
	compression := u.compression && serverAcceptsDeflateOffer(r.Header)

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return u.fail(w, http.StatusInternalServerError, "websocket: response writer does not support hijacking")
	}
	netConn, brw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	b.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Accept: ")
	b.WriteString(computeAcceptKey(challengeKey))
	b.WriteString("\r\n")
	if subprotocol != "" {
		b.WriteString("Sec-WebSocket-Protocol: ")
		b.WriteString(subprotocol)
		b.WriteString("\r\n")
	}
	if compression {
		b.WriteString("Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n")
	}
	b.WriteString("\r\n")

	if u.handshakeTimeout > 0 {
		_ = netConn.SetWriteDeadline(time.Now().Add(u.handshakeTimeout))
	}
	if _, err := netConn.Write([]byte(b.String())); err != nil {
		_ = netConn.Close()
		return nil, err
	}
	_ = netConn.SetWriteDeadline(time.Time{})

	conn := newConn(netConn, true, brw.Reader, subprotocol, compression, u.compressionLevel)
	if u.readLimit > 0 {
		conn.readLimit = u.readLimit
	}
	return conn, nil
}

// fail writes an HTTP error response and returns a HandshakeError.
func (u *Upgrader) fail(w http.ResponseWriter, status int, reason string) (*Conn, error) {
	http.Error(w, http.StatusText(status), status)
	return nil, HandshakeError{message: reason}
}

// selectSubprotocol returns the first server subprotocol that the client also
// requested, honouring server preference order.
func (u *Upgrader) selectSubprotocol(r *http.Request) string {
	if len(u.subprotocols) == 0 {
		return ""
	}
	requested := Subprotocols(r)
	for _, s := range u.subprotocols {
		for _, c := range requested {
			if s == c {
				return s
			}
		}
	}
	return ""
}

// checkSameOrigin is the default origin policy: it accepts a request whose
// Origin host matches the Host header, and requests without an Origin (typical
// of non-browser clients). This blocks cross-site WebSocket hijacking from
// browsers.
func checkSameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, r.Host)
}
