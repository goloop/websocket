package websocket

import (
	"compress/flate"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// HandshakeError describes a failed WebSocket upgrade. Upgrade returns it as a
// *HandshakeError, so errors.As(err, &he) with a *HandshakeError finds it, and
// it has already written an HTTP error response to the client by then.
//
// Status is the HTTP status that was written, which classifies the failure
// without reading the message: 405 for the wrong method, 400 for a malformed
// request, 426 for the wrong version, 403 for a rejected origin and 500 for a
// response writer that cannot be hijacked.
type HandshakeError struct {
	Status  int
	message string
}

// Error implements the error interface, returning the reason the upgrade failed.
func (e *HandshakeError) Error() string { return e.message }

// Upgrader holds a reusable server-side upgrade configuration.
type Upgrader struct {
	cfgErr           error
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
	if u.cfgErr != nil {
		// A bad option is the server's own fault, and saying so on the first
		// request beats compressing with the wrong level for a year.
		return u.fail(w, http.StatusInternalServerError, u.cfgErr.Error())
	}
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
	if !u.originCheck()(r) {
		return u.fail(w, http.StatusForbidden, "websocket: origin not allowed")
	}

	subprotocol := u.selectSubprotocol(r)
	compression := u.compression && serverAcceptsDeflateOffer(r.Header)

	// The 101 is written by hand, so anything the handler or the middleware
	// put on the ResponseWriter, a session cookie above all, would be lost
	// unless it is carried over here. The protocol's own headers are set
	// below and are not taken from the writer.
	extra, err := carriedHeaders(w.Header())
	if err != nil {
		return u.fail(w, http.StatusInternalServerError, err.Error())
	}

	// Hijack through http.ResponseController so the upgrade also works behind
	// middleware that wraps the ResponseWriter and exposes only Unwrap (the
	// stdlib-idiomatic pattern since Go 1.20), not a direct http.Hijacker.
	netConn, brw, herr := http.NewResponseController(w).Hijack()
	err = herr
	if err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			return u.fail(w, http.StatusInternalServerError, "websocket: response writer does not support hijacking")
		}
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
	b.WriteString(extra)
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

// fail writes an HTTP error response and returns a *HandshakeError.
func (u *Upgrader) fail(w http.ResponseWriter, status int, reason string) (*Conn, error) {
	http.Error(w, http.StatusText(status), status)
	return nil, &HandshakeError{Status: status, message: reason}
}

// reservedResponseHeaders are written by the handshake itself, so a value left
// on the ResponseWriter for one of them is ignored rather than sent twice.
var reservedResponseHeaders = map[string]bool{
	"Upgrade":                  true,
	"Connection":               true,
	"Sec-Websocket-Accept":     true,
	"Sec-Websocket-Protocol":   true,
	"Sec-Websocket-Extensions": true,
	"Sec-Websocket-Version":    true,
	"Content-Length":           true,
	"Content-Type":             true,
	"Transfer-Encoding":        true,
}

// carriedHeaders renders the headers the handler set on the ResponseWriter so
// they can be appended to the 101. A name or value that could break the
// response apart is refused rather than written: this text goes onto the wire
// verbatim, so a newline in it would be a response-splitting injection.
func carriedHeaders(h http.Header) (string, error) {
	if len(h) == 0 {
		return "", nil
	}

	names := make([]string, 0, len(h))
	for name := range h {
		if !reservedResponseHeaders[http.CanonicalHeaderKey(name)] {
			names = append(names, name)
		}
	}
	slices.Sort(names) // a stable response is easier to test and to read

	var b strings.Builder
	for _, name := range names {
		if !validHeaderName(name) {
			return "", errors.New("websocket: invalid response header name")
		}
		for _, v := range h[name] {
			if !validHeaderValue(v) {
				return "", errors.New("websocket: invalid response header value")
			}
			b.WriteString(name)
			b.WriteString(": ")
			b.WriteString(v)
			b.WriteString("\r\n")
		}
	}
	return b.String(), nil
}

// isTokenByte reports whether c may appear in an RFC 9110 token, which is what
// a header name and a subprotocol name both are.
func isTokenByte(c byte) bool {
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.',
		'^', '_', '`', '|', '~':
		return true
	}
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// validHeaderName reports whether name is an RFC 9110 field name (a token).
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenByte(name[i]) {
			return false
		}
	}
	return true
}

// validHeaderValue reports whether value can be written as a field value: no
// control characters, and nothing that could start a new line or header.
func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// originCheck returns the origin policy in force. A zero Upgrader, and an
// explicit nil from WithOriginChecker, both mean the default same-origin
// policy: the safe reading of "no policy was set", and the one that does not
// panic on the first valid request.
func (u *Upgrader) originCheck() func(*http.Request) bool {
	if u.originChecker == nil {
		return checkSameOrigin
	}
	return u.originChecker
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

// checkSameOrigin is the default origin policy. It accepts a request with no
// Origin at all, which is what a non-browser client sends, and otherwise
// requires a well-formed origin whose host matches the Host header.
//
// What it checks: exactly one Origin header, a serialized origin and nothing
// more (no path, query, fragment or userinfo), a scheme of http or https, and
// a matching host. An opaque origin, which a browser sends as "null" from a
// sandboxed frame or a file, is refused rather than treated as same-origin.
// When the request arrived over TLS directly, the origin must be https, so an
// http page cannot reach a wss endpoint on the same host.
//
// What it does not check: the port, unless both sides state one. Behind a
// reverse proxy the server sees neither the external scheme nor the external
// port, so deriving a default port here would reject the ordinary deployment
// where TLS is terminated in front. A service that needs the port, or the
// scheme, to be part of the decision should pass its own allowlist to
// [WithOriginChecker].
func checkSameOrigin(r *http.Request) bool {
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true // not a browser request
	}
	if len(origins) > 1 {
		return false // a request carries at most one origin
	}

	origin := origins[0]
	if strings.EqualFold(origin, "null") {
		return false // an opaque origin is not this one
	}

	u, err := url.Parse(origin)
	if err != nil || u.Host == "" ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		if r.TLS != nil {
			return false // an http page must not reach a TLS endpoint
		}
	case "https":
	default:
		return false
	}

	originHost, originPort := splitHostPort(u.Host)
	requestHost, requestPort := splitHostPort(r.Host)
	if !strings.EqualFold(originHost, requestHost) {
		return false
	}
	if originPort != "" && requestPort != "" {
		return originPort == requestPort
	}
	return true
}

// splitHostPort separates a host from an explicit port, returning an empty
// port when none was written. It keeps a bracketed IPv6 literal intact.
func splitHostPort(hostport string) (host, port string) {
	if i := strings.LastIndexByte(hostport, ':'); i != -1 {
		if !strings.Contains(hostport[i+1:], "]") {
			return hostport[:i], hostport[i+1:]
		}
	}
	return hostport, ""
}
