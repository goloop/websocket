// Package websocket implements the WebSocket protocol (RFC 6455) on top of the
// standard library, with no third-party dependencies. It provides a server-side
// upgrade, a client-side dial, the permessage-deflate extension (RFC 7692) and
// subprotocol negotiation.
//
// A connection is represented by Conn. Because the package name is websocket,
// the natural variable name for a connection is ws:
//
//	ws, err := websocket.Upgrade(w, r)
//	if err != nil {
//	    return
//	}
//	defer ws.Close()
//	for {
//	    mt, data, err := ws.ReadMessage()
//	    if err != nil {
//	        break
//	    }
//	    if err := ws.WriteMessage(mt, data); err != nil {
//	        break
//	    }
//	}
//
// Server upgrade. Use the package Upgrade for a one-off, or NewUpgrader for a
// reusable configuration:
//
//	up := websocket.NewUpgrader(websocket.WithReadLimit(1 << 20))
//	ws, err := up.Upgrade(w, r)
//
// By default the upgrade only accepts same-origin requests, which guards
// against cross-site WebSocket hijacking: one well-formed Origin whose host
// matches Host, an http or https scheme, and https when the request arrived
// over TLS directly. Ports are compared only when both sides state one, since
// behind a reverse proxy the server sees neither the external scheme nor the
// external port. Allow other origins explicitly with WithOriginChecker, which
// is also where an allowlist belongs. A failed upgrade returns a
// *HandshakeError, whose Status is the HTTP status that was written.
//
// Client dial. Dial connects to a server, negotiating TLS for wss URLs:
//
//	ws, resp, err := websocket.Dial(ctx, "wss://example.com/ws")
//
// Once the TCP connection is up, the whole handshake runs under one budget:
// the earlier of the context deadline and WithDialHandshakeTimeout, and at
// most WithDialHandshakeLimit bytes of response. Cancelling the context ends
// the handshake at once rather than at its deadline.
//
// Concurrency. A connection supports one concurrent reader and one concurrent
// writer. That is, ReadMessage or NextReader may run in one goroutine while
// WriteMessage or NextWriter runs in another, but you must not call two readers
// or two writers at the same time. WriteControl may be called concurrently with
// a writer, and works between the fragments of a message being streamed.
//
// Streaming and limits. Reading streams; so does writing. NextWriter sends
// fragments as they fill, so a message larger than memory is a matter of
// writing it, and a message that fits in one fragment is still sent as a
// single frame. Only one message may be in flight: from its first fragment
// until Close, another data write returns ErrMessageInFlight. SetReadLimit
// caps a received message (32 MiB by default) and SetWriteLimit caps a sent
// one (unbounded by default).
//
// Errors. How a connection ended is always a *CloseError: the code and reason
// the peer sent, or 1006 with the underlying error as its cause for every
// ending without a closing handshake, a network failure such as a reset
// included. A read deadline is not an ending and stays the timeout it was.
// Use IsCloseError and IsUnexpectedCloseError in a read loop. A message cut off between fragments
// is never returned as a whole message; that read matches io.ErrUnexpectedEOF
// and the failure is sticky. ErrProtocol matches any framing violation by the
// peer and ErrReadLimit a message that was too big.
//
// Control frames. Ping, pong and close frames are handled by the reader: a ping
// is answered with a pong automatically, and a close starts the closing
// handshake. Install SetPingHandler, SetPongHandler or SetCloseHandler to
// observe or override this.
package websocket
