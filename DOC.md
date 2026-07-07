# websocket - reference

A from-scratch WebSocket (RFC 6455) implementation on the standard library, with
no third-party dependencies.

## Table of contents

- [Server upgrade](#server-upgrade)
- [Client dial](#client-dial)
- [Reading and writing](#reading-and-writing)
- [Control frames and close](#control-frames-and-close)
- [Compression](#compression)
- [Limits and deadlines](#limits-and-deadlines)
- [Concurrency](#concurrency)
- [Errors](#errors)

## Server upgrade

```go
ws, err := websocket.Upgrade(w, r, opts...)          // one-off
up := websocket.NewUpgrader(opts...); ws, err := up.Upgrade(w, r) // reusable
```

On failure `Upgrade` writes an HTTP error response and returns a
`*HandshakeError`. Options:

- `WithOriginChecker(fn)` - decide whether a request's Origin is allowed. The
  default (`checkSameOrigin`) accepts same-origin requests and requests without
  an Origin header, blocking cross-site WebSocket hijacking.
- `WithSubprotocols(names...)` - subprotocols the server supports, in order of
  preference; the first the client also offers is selected.
- `WithReadLimit(bytes)` - maximum size of a received message.
- `WithCompression()` / `WithCompressionLevel(level)` - enable
  permessage-deflate.
- `WithHandshakeTimeout(d)` - bound the response write.

Helpers: `IsWebSocketUpgrade(r)`, `Subprotocols(r)`.

## Client dial

```go
ws, resp, err := websocket.Dial(ctx, "wss://host/path", opts...)
```

The scheme must be `ws` or `wss`; `wss` uses TLS. A non-101 reply returns
`ErrBadHandshake` together with the `*http.Response`. Options:

- `WithDialHeader(h)` - extra request headers (Authorization, Cookie, Origin).
- `WithDialSubprotocols(names...)`.
- `WithDialTLSConfig(cfg)` - TLS settings for `wss`.
- `WithDialNetDialer(d)` - the `net.Dialer` used for the TCP connection.
- `WithDialCompression()` - offer permessage-deflate.

## Reading and writing

Whole messages:

```go
mt, data, err := ws.ReadMessage()          // mt is TextMessage or BinaryMessage
err = ws.WriteMessage(websocket.TextMessage, data)
```

Streaming (uncompressed messages are streamed; a compressed message is inflated
in full first):

```go
mt, r, err := ws.NextReader()   // r is an io.Reader
w, err := ws.NextWriter(websocket.BinaryMessage) // w is an io.WriteCloser; Close sends
```

JSON:

```go
err = ws.WriteJSON(v)
err = ws.ReadJSON(&v)
```

A received text message is validated as UTF-8; invalid text closes the
connection with `1007`.

## Control frames and close

Ping, pong and close frames are handled by the reader. A ping is answered with a
pong automatically; a close begins the closing handshake. Override with
`SetPingHandler`, `SetPongHandler`, `SetCloseHandler`.

Send your own:

```go
ws.WriteControl(websocket.PingMessage, []byte("hi"), time.Now().Add(time.Second))
ws.CloseWithStatus(websocket.CloseNormalClosure, "bye")
```

`CloseWithStatus` sends a close frame but does not close the socket; after the
peer's close arrives (the reader returns a `*CloseError`), call `Close` to
release the connection. `Close` on its own closes the socket without a handshake.

## Compression

permessage-deflate (RFC 7692) is negotiated during the handshake when both sides
enable it. It uses "no context takeover": each message is compressed
independently. The read limit is enforced on the *decompressed* size, guarding
against decompression bombs.

## Limits and deadlines

- `SetReadLimit(n)` caps a single message (default 32 MiB).
- `SetReadDeadline` / `SetWriteDeadline` bound I/O; use them so a slow or stuck
  peer cannot block a goroutine indefinitely.

## Concurrency

One reader and one writer may run concurrently. `WriteControl` is safe to call
from a goroutine other than the writer. Two simultaneous readers, or two
writers, are not supported.

## Errors

- `*CloseError{Code, Text}` - the peer closed. Use `IsCloseError(err, codes...)`
  and `IsUnexpectedCloseError(err, expected...)` in a read loop.
- `ErrBadHandshake` - the client handshake was rejected.
- `ErrCloseSent` - a write after the closing handshake began.
- `*HandshakeError` - the server upgrade failed (an HTTP error was written).
