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
// By default the upgrade only accepts same-origin requests, which guards against
// cross-site WebSocket hijacking. Allow other origins explicitly with
// WithOriginChecker.
//
// Client dial. Dial connects to a server, negotiating TLS for wss URLs:
//
//	ws, resp, err := websocket.Dial(ctx, "wss://example.com/ws")
//
// Concurrency. A connection supports one concurrent reader and one concurrent
// writer. That is, ReadMessage or NextReader may run in one goroutine while
// WriteMessage or NextWriter runs in another, but you must not call two readers
// or two writers at the same time. WriteControl may be called concurrently with
// a writer.
//
// Control frames. Ping, pong and close frames are handled by the reader: a ping
// is answered with a pong automatically, and a close starts the closing
// handshake. Install SetPingHandler, SetPongHandler or SetCloseHandler to
// observe or override this.
package websocket
