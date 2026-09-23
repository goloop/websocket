package websocket

import (
	"compress/flate"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// Option configures an Upgrader (server side).
type Option func(*Upgrader)

// WithOriginChecker sets the function that decides whether a request's Origin is
// allowed. The default accepts only same-origin requests, which guards against
// cross-site WebSocket hijacking. To allow any origin (for example a public API
// or local development) pass a function that always returns true.
func WithOriginChecker(fn func(*http.Request) bool) Option {
	return func(u *Upgrader) { u.originChecker = fn }
}

// WithSubprotocols sets the subprotocols the server supports, in order of
// preference. The first one the client also offers is selected.
func WithSubprotocols(names ...string) Option {
	return func(u *Upgrader) { u.subprotocols = names }
}

// WithReadLimit sets the maximum size of a single received message.
func WithReadLimit(bytes int64) Option {
	return func(u *Upgrader) { u.readLimit = bytes }
}

// WithCompression enables the permessage-deflate extension when the client
// offers it.
func WithCompression() Option {
	return func(u *Upgrader) { u.compression = true }
}

// WithCompressionLevel sets the flate level used for outgoing messages
// (flate.HuffmanOnly to flate.BestCompression).
func WithCompressionLevel(level int) Option {
	return func(u *Upgrader) { u.compressionLevel = level }
}

// WithHandshakeTimeout bounds the time spent writing the upgrade response.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(u *Upgrader) { u.handshakeTimeout = d }
}

// DialOption configures a client Dial.
type DialOption func(*dialConfig)

type dialConfig struct {
	header           http.Header
	subprotocols     []string
	tlsConfig        *tls.Config
	netDialer        *net.Dialer
	compression      bool
	compressionLevel int
	handshakeTimeout time.Duration
	handshakeLimit   int64
}

// defaultHandshakeLimit is how many bytes of a server's handshake response
// Dial reads before giving up. A real 101 reply is a fraction of this; the
// budget exists so a server that never stops sending headers cannot spend the
// client's memory.
const defaultHandshakeLimit = 64 * 1024

func newDialConfig() *dialConfig {
	return &dialConfig{
		compressionLevel: flate.DefaultCompression,
		netDialer:        &net.Dialer{Timeout: 45 * time.Second},
		handshakeTimeout: 45 * time.Second,
		handshakeLimit:   defaultHandshakeLimit,
	}
}

// WithDialHandshakeTimeout bounds the time spent establishing the connection
// once the TCP dial has succeeded: the TLS handshake for wss, writing the
// request and reading the response. The default is 45s; a value <= 0 leaves
// the context as the only bound.
//
// When the context carries a deadline of its own, the earlier of the two
// applies, so a long-lived context cannot quietly replace a short timeout.
func WithDialHandshakeTimeout(d time.Duration) DialOption {
	return func(c *dialConfig) { c.handshakeTimeout = d }
}

// WithDialHandshakeLimit bounds how many bytes of the server's handshake
// response Dial reads before failing with [ErrHandshakeTooLarge]. The default
// is 64 KiB; a value <= 0 removes the bound. It applies only to the handshake,
// so it does not limit messages: use [Conn.SetReadLimit] for those.
func WithDialHandshakeLimit(bytes int64) DialOption {
	return func(c *dialConfig) { c.handshakeLimit = bytes }
}

// WithDialHeader adds extra HTTP headers to the client handshake request (for
// example Authorization or Cookie).
func WithDialHeader(h http.Header) DialOption {
	return func(c *dialConfig) { c.header = h }
}

// WithDialSubprotocols sets the subprotocols the client offers.
func WithDialSubprotocols(names ...string) DialOption {
	return func(c *dialConfig) { c.subprotocols = names }
}

// WithDialTLSConfig sets the TLS configuration used for wss URLs.
func WithDialTLSConfig(cfg *tls.Config) DialOption {
	return func(c *dialConfig) { c.tlsConfig = cfg }
}

// WithDialNetDialer sets the net.Dialer used to establish the TCP connection.
func WithDialNetDialer(d *net.Dialer) DialOption {
	return func(c *dialConfig) {
		if d != nil {
			c.netDialer = d
		}
	}
}

// WithDialCompression offers the permessage-deflate extension to the server.
func WithDialCompression() DialOption {
	return func(c *dialConfig) { c.compression = true }
}
