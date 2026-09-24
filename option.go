package websocket

import (
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"slices"
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
//
// Each name must be an HTTP token, since it is sent in a header; anything
// else fails the upgrade with [ErrConfig] instead of putting a malformed
// header on the wire. The slice is copied, so the caller may reuse it.
func WithSubprotocols(names ...string) Option {
	return func(u *Upgrader) {
		if err := validSubprotocols(names); err != nil {
			u.cfgErr = err
			return
		}
		u.subprotocols = slices.Clone(names)
	}
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
// (flate.HuffmanOnly to flate.BestCompression). A level outside that range
// fails the upgrade with [ErrConfig] rather than being silently replaced by
// the default, which used to hide a typo behind working compression.
func WithCompressionLevel(level int) Option {
	return func(u *Upgrader) {
		if !isValidCompressionLevel(level) {
			u.cfgErr = fmt.Errorf("%w: compression level %d", ErrConfig, level)
			return
		}
		u.compressionLevel = level
	}
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
	readLimit        int64
	cfgErr           error
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
// example Authorization or Cookie). The header is cloned, so the caller may
// go on using and changing its own copy.
func WithDialHeader(h http.Header) DialOption {
	return func(c *dialConfig) { c.header = h.Clone() }
}

// WithDialSubprotocols sets the subprotocols the client offers. Each name must
// be an HTTP token, and the slice is copied.
func WithDialSubprotocols(names ...string) DialOption {
	return func(c *dialConfig) {
		if err := validSubprotocols(names); err != nil {
			c.cfgErr = err
			return
		}
		c.subprotocols = slices.Clone(names)
	}
}

// WithDialTLSConfig sets the TLS configuration used for wss URLs. It is cloned
// on the way in, so a later change by the caller cannot alter a dial that is
// already under way.
func WithDialTLSConfig(cfg *tls.Config) DialOption {
	return func(c *dialConfig) {
		if cfg != nil {
			cfg = cfg.Clone()
		}
		c.tlsConfig = cfg
	}
}

// WithDialReadLimit sets the maximum size of a received message on the
// connection Dial returns, the client-side counterpart of [WithReadLimit].
func WithDialReadLimit(bytes int64) DialOption {
	return func(c *dialConfig) { c.readLimit = bytes }
}

// WithDialCompressionLevel sets the flate level used for outgoing messages,
// the client-side counterpart of [WithCompressionLevel].
func WithDialCompressionLevel(level int) DialOption {
	return func(c *dialConfig) {
		if !isValidCompressionLevel(level) {
			c.cfgErr = fmt.Errorf("%w: compression level %d", ErrConfig, level)
			return
		}
		c.compressionLevel = level
	}
}

// validSubprotocols reports whether every name may be written in a
// Sec-WebSocket-Protocol header.
func validSubprotocols(names []string) error {
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("%w: empty subprotocol name", ErrConfig)
		}
		for i := 0; i < len(name); i++ {
			if !isTokenByte(name[i]) {
				return fmt.Errorf("%w: subprotocol %q is not a token",
					ErrConfig, name)
			}
		}
	}
	return nil
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
