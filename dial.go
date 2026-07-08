package websocket

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// ErrBadHandshake is returned by Dial when the server does not complete the
// WebSocket handshake. The returned *http.Response carries the server's reply so
// the caller can inspect the status and headers.
var ErrBadHandshake = errors.New("websocket: bad handshake")

// Dial connects to a WebSocket server. The URL scheme must be ws or wss; wss
// connections are negotiated over TLS. On success it returns the connection and
// the server's handshake response. On a non-101 reply it returns ErrBadHandshake
// together with the response.
func Dial(ctx context.Context, urlStr string, opts ...DialOption) (*Conn, *http.Response, error) {
	cfg := newDialConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}

	var secure bool
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
		secure = true
	default:
		return nil, nil, errors.New("websocket: unsupported URL scheme")
	}

	hostPort := u.Host
	if u.Port() == "" {
		if secure {
			hostPort += ":443"
		} else {
			hostPort += ":80"
		}
	}

	netConn, err := cfg.netDialer.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return nil, nil, err
	}
	if secure {
		tlsConn := tls.Client(netConn, tlsClientConfig(cfg.tlsConfig, u.Hostname()))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = netConn.Close()
			return nil, nil, err
		}
		netConn = tlsConn
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = netConn.SetDeadline(deadline)
	} else if cfg.handshakeTimeout > 0 {
		_ = netConn.SetDeadline(time.Now().Add(cfg.handshakeTimeout))
	}

	challengeKey, err := generateChallengeKey()
	if err != nil {
		_ = netConn.Close()
		return nil, nil, err
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    u,
		Host:   u.Host,
		Header: http.Header{},
	}
	for k, vs := range cfg.header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", challengeKey)
	if len(cfg.subprotocols) > 0 {
		req.Header.Set("Sec-WebSocket-Protocol", strings.Join(cfg.subprotocols, ", "))
	}
	if cfg.compression {
		req.Header.Set("Sec-WebSocket-Extensions",
			"permessage-deflate; client_no_context_takeover; server_no_context_takeover")
	}

	if err := req.Write(netConn); err != nil {
		_ = netConn.Close()
		return nil, nil, err
	}

	br := bufio.NewReader(netConn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = netConn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols ||
		!tokenListContainsValue(resp.Header, "Connection", "upgrade") ||
		!tokenListContainsValue(resp.Header, "Upgrade", "websocket") ||
		resp.Header.Get("Sec-WebSocket-Accept") != computeAcceptKey(challengeKey) {
		_ = netConn.Close()
		return nil, resp, ErrBadHandshake
	}

	// A server must not select a subprotocol the client did not offer.
	subprotocol := resp.Header.Get("Sec-WebSocket-Protocol")
	if subprotocol != "" && !slices.Contains(cfg.subprotocols, subprotocol) {
		_ = netConn.Close()
		return nil, resp, ErrBadHandshake
	}

	// Validate the negotiated permessage-deflate parameters, failing on a
	// response this client cannot honour (for example missing
	// server_no_context_takeover or an unknown parameter).
	compression := false
	if cfg.compression {
		ok, err := clientAcceptsDeflateResponse(resp.Header)
		if err != nil {
			_ = netConn.Close()
			return nil, resp, err
		}
		compression = ok
	}

	_ = netConn.SetDeadline(time.Time{})
	conn := newConn(netConn, false, br, subprotocol, compression, cfg.compressionLevel)
	return conn, resp, nil
}

// tlsClientConfig returns a TLS config for the dial, defaulting the server name
// to host when not already set.
func tlsClientConfig(cfg *tls.Config, host string) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}
	return cfg
}
