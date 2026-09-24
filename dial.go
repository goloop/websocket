package websocket

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// ErrBadHandshake is returned by Dial when the server does not complete the
// WebSocket handshake. The returned *http.Response carries the server's reply so
// the caller can inspect the status and headers.
var ErrBadHandshake = errors.New("websocket: bad handshake")

// ErrHandshakeTooLarge is returned by Dial when the server's handshake
// response exceeds the byte budget set by [WithDialHandshakeLimit]. Nothing
// bounds an HTTP response read this way on its own, so without a budget a
// server could spend the client's memory before the handshake timeout was
// anywhere near expiring.
var ErrHandshakeTooLarge = errors.New("websocket: handshake response too large")

// Dial connects to a WebSocket server. The URL scheme must be ws or wss; wss
// connections are negotiated over TLS. On success it returns the connection and
// the server's handshake response. On a non-101 reply it returns ErrBadHandshake
// together with the response.
func Dial(ctx context.Context, urlStr string, opts ...DialOption) (*Conn, *http.Response, error) {
	cfg := newDialConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.cfgErr != nil {
		return nil, nil, cfg.cfgErr
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

	// Everything from here to the established connection runs under one
	// budget and one cancellation. Until the handshake succeeds the socket
	// belongs to this function, so every path out of it closes the socket
	// rather than leaving it, and the caller's goroutine, to a silent peer.
	// established is set at the one success point and read under cancelMu,
	// because the cancellation callback below reads it from its own
	// goroutine.
	var cancelMu sync.Mutex
	established := false
	defer func() {
		cancelMu.Lock()
		done := established
		cancelMu.Unlock()
		if !done {
			_ = netConn.Close()
		}
	}()

	// The deadline covers the TLS handshake too. Setting it only afterwards
	// left TLS bounded by nothing but the TCP dial timeout, so a peer that
	// completed TCP and then said nothing held the caller indefinitely.
	if deadline, ok := handshakeDeadline(ctx, cfg.handshakeTimeout); ok {
		if err := netConn.SetDeadline(deadline); err != nil {
			return nil, nil, err
		}
	}

	// A deadline is not cancellation: ctx may be cancelled long before it.
	// Closing the socket is what makes the blocked I/O return, so the
	// handshake answers cancel() at once instead of at its deadline. The
	// callback closes the connection the dial produced, which unblocks the
	// TLS handshake layered on top of it too; it is captured by value so
	// that wrapping netConn in a tls.Conn is not a write this callback could
	// be reading at the same time.
	rawConn := netConn
	stopCancel := context.AfterFunc(ctx, func() {
		cancelMu.Lock()
		defer cancelMu.Unlock()
		if !established {
			_ = rawConn.Close()
		}
	})
	defer stopCancel()

	// fail reports what actually ended the handshake. When the context is
	// done, the I/O error is a side effect of this function closing the
	// socket, so the caller is told about the cancellation it asked for and
	// errors.Is(err, context.Canceled) works.
	fail := func(cause error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("websocket: handshake: %w", ctxErr)
		}
		return cause
	}

	if secure {
		tlsConn := tls.Client(netConn, tlsClientConfig(cfg.tlsConfig, u.Hostname()))
		if herr := tlsConn.HandshakeContext(ctx); herr != nil {
			return nil, nil, fail(herr)
		}
		netConn = tlsConn
	}

	challengeKey, err := generateChallengeKey()
	if err != nil {
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

	if werr := req.Write(netConn); werr != nil {
		return nil, nil, fail(werr)
	}

	// The response is read through a budget. http.ReadResponse imposes no
	// limit of its own here (no http.Transport is involved), so without one
	// a hostile or broken server could stream headers until the client ran
	// out of memory. The budget is lifted once the handshake is done, and the
	// same bufio.Reader carries any WebSocket bytes that arrived with it.
	hr := &handshakeReader{r: netConn, remaining: cfg.handshakeLimit,
		bounded: cfg.handshakeLimit > 0}
	br := bufio.NewReader(hr)
	resp, rerr := http.ReadResponse(br, req)
	if rerr != nil {
		if errors.Is(rerr, ErrHandshakeTooLarge) {
			return nil, nil, ErrHandshakeTooLarge
		}
		return nil, nil, fail(rerr)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols ||
		!tokenListContainsValue(resp.Header, "Connection", "upgrade") ||
		!tokenListContainsValue(resp.Header, "Upgrade", "websocket") ||
		resp.Header.Get("Sec-WebSocket-Accept") != computeAcceptKey(challengeKey) {
		return nil, resp, ErrBadHandshake
	}

	// Accept and Protocol carry one value each. Header.Get would quietly read
	// the first of several, which is how a response that says two different
	// things gets treated as if it said the acceptable one.
	if len(resp.Header.Values("Sec-WebSocket-Accept")) != 1 ||
		len(resp.Header.Values("Sec-WebSocket-Protocol")) > 1 {
		return nil, resp, ErrBadHandshake
	}

	// A server must not select a subprotocol the client did not offer.
	subprotocol := resp.Header.Get("Sec-WebSocket-Protocol")
	if subprotocol != "" && !slices.Contains(cfg.subprotocols, subprotocol) {
		return nil, resp, ErrBadHandshake
	}

	// Validate the extensions the server selected. This runs whether or not
	// compression was offered: a server that selects an extension the client
	// never proposed is not speaking the connection this client asked for,
	// and the frames that follow may not be readable at all.
	compression, derr := validateServerExtensions(resp.Header, cfg.compression)
	if derr != nil {
		return nil, resp, derr
	}

	hr.release()
	_ = netConn.SetDeadline(time.Time{})
	conn := newConn(netConn, false, br, subprotocol, compression, cfg.compressionLevel)
	if cfg.readLimit > 0 {
		conn.SetReadLimit(cfg.readLimit)
	}

	// The connection is the caller's from here: the deferred close must not
	// take it, and a cancellation callback that fires from now on must not
	// close a connection that has been handed over.
	cancelMu.Lock()
	established = true
	cancelMu.Unlock()

	return conn, resp, nil
}

// handshakeDeadline returns the moment the handshake must be done by: the
// earlier of the context's own deadline and the configured timeout. Taking the
// earlier of the two means a generous context cannot quietly replace a short
// timeout, and a short context is still honoured.
func handshakeDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	deadline, ok := ctx.Deadline()
	if timeout > 0 {
		t := time.Now().Add(timeout)
		if !ok || t.Before(deadline) {
			deadline, ok = t, true
		}
	}
	return deadline, ok
}

// handshakeReader bounds how much of the server's handshake response is read
// into memory. Once the handshake is done the bound is lifted in place rather
// than by swapping the reader, so the bytes the buffered reader has already
// pulled from the socket, which may include the first WebSocket frames, are
// not lost.
type handshakeReader struct {
	r         io.Reader
	remaining int64
	bounded   bool
}

// Read reads from the underlying connection while the budget lasts, and fails
// with ErrHandshakeTooLarge once it is spent.
func (h *handshakeReader) Read(p []byte) (int, error) {
	if !h.bounded {
		return h.r.Read(p)
	}
	if h.remaining <= 0 {
		return 0, ErrHandshakeTooLarge
	}
	if int64(len(p)) > h.remaining {
		p = p[:h.remaining]
	}
	n, err := h.r.Read(p)
	h.remaining -= int64(n)
	return n, err
}

// release lifts the budget, leaving the connection readable without a bound.
func (h *handshakeReader) release() { h.bounded = false }

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
