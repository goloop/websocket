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
`*HandshakeError`, whose `Status` is the HTTP status that was written.
Headers the handler set on the `ResponseWriter` before upgrading, a session
cookie above all, are carried into the `101`; the protocol's own headers are
not taken from the writer, and a header whose name or value could split the
response fails the upgrade with `500`.

Options:

- `WithOriginChecker(fn)` - decide whether a request's Origin is allowed.
  Passing `nil` restores the default. That default requires exactly one
  well-formed `Origin` whose host matches `Host`, refuses an opaque (`null`)
  origin and a scheme other than http or https, and, when the request arrived
  over TLS directly, requires an https origin. It compares ports only when
  both sides state one, because behind a reverse proxy the server sees neither
  the external scheme nor the external port; a service that needs those in the
  decision passes its own allowlist here. A request with no Origin, which is
  what a non-browser client sends, is accepted.
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
- `WithDialHandshakeTimeout(d)` - bound the TLS handshake, the request and the
  response (default 45s).
- `WithDialHandshakeLimit(n)` - bound the bytes read from the handshake
  response (default 64 KiB), failing with `ErrHandshakeTooLarge`.
- `WithDialReadLimit(bytes)` / `WithDialCompressionLevel(level)` - the
  client-side counterparts of `WithReadLimit` and `WithCompressionLevel`.

`Conn.CompressionEnabled()` reports whether permessage-deflate was actually
negotiated, which is not the same as having asked for it.

An option given a value this package cannot use - a compression level out of
range, a subprotocol that is not an HTTP token - fails with `ErrConfig` from
`Dial`, or a `500` from `Upgrade`, instead of being replaced by a default.
Slices, headers and TLS configs passed to options are copied, so the caller
may keep using its own.

Once the TCP connection is up, the whole handshake runs under one budget: the
earlier of the context deadline and the handshake timeout, so a long-lived
context cannot quietly replace a short timeout. Cancelling the context ends the
handshake straight away rather than at its deadline, and the error then matches
`context.Canceled`. A connection that was handed back is the caller's: a
context cancelled afterwards does not close it.

## Reading and writing

Whole messages:

```go
mt, data, err := ws.ReadMessage()          // mt is TextMessage or BinaryMessage
err = ws.WriteMessage(websocket.TextMessage, data)
```

Both directions stream. A compressed message is inflated in full before
reading; `NextWriter` sends fragments as they fill, so a message larger than
memory is a matter of writing it.

```go
mt, r, err := ws.NextReader()   // r is an io.Reader
w, err := ws.NextWriter(websocket.BinaryMessage) // w is an io.WriteCloser; Close sends
```

A message that fits in one fragment (32 KiB) is still sent as a single
unfragmented frame when the writer is closed, so nothing changes on the wire
for ordinary messages. Larger ones go out as they are written, and only a
fragment's worth is ever held.

Only one message may be in flight. From the first fragment until `Close`,
another `WriteMessage` or `NextWriter` returns `ErrMessageInFlight`, because
its frames would be read as continuations of the open message. Control frames
are unaffected and still go out between fragments, so pings, pongs and closes
keep working while a long message is being sent. `Close` must be called to end
the message.

`SetWriteLimit` still bounds the message as a whole, and refuses before any
part of an oversized one reaches the wire.

A reader is valid for one message: once `NextReader` or `ReadMessage` has moved
on, the old one fails with `ErrStaleReader` rather than serving bytes of the
next message. `NextWriter` refuses a connection whose writes have already
failed, or that has sent a close, instead of accepting a whole message and
failing at `Close`; and `Close` returns the same result however often it is
called, so a deferred `Close` after a failed one does not report success.

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

### Close codes

The codes of RFC 6455 and the IANA registry are named constants:

| Constant | Code | |
|---|---:|---|
| `CloseNormalClosure` | 1000 | |
| `CloseGoingAway` | 1001 | |
| `CloseProtocolError` | 1002 | sent for a framing violation |
| `CloseUnsupportedData` | 1003 | |
| `CloseNoStatusReceived` | 1005 | local only |
| `CloseAbnormalClosure` | 1006 | local only: the connection dropped |
| `CloseInvalidFramePayloadData` | 1007 | sent for invalid UTF-8 |
| `ClosePolicyViolation` | 1008 | |
| `CloseMessageTooBig` | 1009 | sent when the read limit is exceeded |
| `CloseMandatoryExtension` | 1010 | |
| `CloseInternalServerErr` | 1011 | |
| `CloseServiceRestart` | 1012 | |
| `CloseTryAgainLater` | 1013 | |
| `CloseBadGateway` | 1014 | |
| `CloseTLSHandshake` | 1015 | local only |

The three marked "local only" describe how a connection ended and are never
put on the wire; sending one is refused with `ErrBadClosePayload`, as is a
reason that is not valid UTF-8 or does not fit a control frame. Codes 3000 to
4999 are available to applications.

## Compression

permessage-deflate (RFC 7692) is negotiated during the handshake when both sides
enable it. It uses "no context takeover": each message is compressed
independently. The read limit is enforced on the *decompressed* size, guarding
against decompression bombs.

## Limits and deadlines

- `SetReadLimit(n)` caps a single message (default 32 MiB). The cap covers the
  whole message, including any part skipped by opening the next reader.
- `SetWriteLimit(n)` caps a single outgoing message, failing with
  `ErrWriteLimit`. The default, 0, means no cap.
- `SetReadDeadline` / `SetWriteDeadline` bound I/O; use them so a slow or stuck
  peer cannot block a goroutine indefinitely. `SetWriteDeadline` also ends a
  write already in progress, as on a `net.Conn`, so it can be called from
  another goroutine to unstick one.
- The deadline passed to `WriteControl` bounds the whole call, including the
  wait for a data write in progress. When it passes first the frame is not
  sent and the error reports `Timeout() == true`. It governs that call: it
  supersedes the deadline the connection already carried, and a concurrent
  `SetWriteDeadline` may shorten it but cannot lift it.

## Concurrency

One reader and one writer may run concurrently. `WriteControl` is safe to call
from a goroutine other than the writer. Two simultaneous readers, or two
writers, are not supported.

## Errors

- `*CloseError{Code, Text}` - how the connection ended. Use
  `IsCloseError(err, codes...)` and `IsUnexpectedCloseError(err, expected...)`
  in a read loop. Every ending without a closing handshake gives code 1006 -
  the stream ending, a stream cut off mid-frame, and a network failure such as
  a reset or a broken pipe - with the underlying error as the cause, so
  `errors.Is(err, io.EOF)` and the like still work. A read deadline is not an
  ending: it stays the timeout it was, since the connection is still there and
  the deadline was yours. 1006 is a local observation and is never sent.
- `ErrReadLimit` - a message exceeded `SetReadLimit`; closed with 1009.
- `ErrProtocol` - the peer broke the framing protocol; closed with 1002. Every
  violation matches this one sentinel, and the error text names the rule.
- `ErrStaleReader` - the reader belongs to a message the connection has
  already moved past.
- `ErrMessageInFlight` - a streamed message has frames on the wire; close its
  writer before starting another.
- `ErrConfig` - an option was given a value this package cannot use.
- `ErrWriteClosed`, `ErrBadControl`, `ErrControlTooBig`, `ErrBadWriteType`,
  `ErrBadClosePayload` - the call's arguments were wrong. Nothing is written
  and the connection is left as it was.
- `ErrBadHandshake` - the client handshake was rejected.
- `ErrHandshakeTooLarge` - the server's handshake response passed the byte
  budget.
- `ErrCloseSent` - a write after the closing handshake began.
- `ErrWriteLimit` - a message passed the limit set by `SetWriteLimit`. Nothing
  was sent and the connection stays usable.
- `*HandshakeError` - the server upgrade failed (an HTTP error was written).
  `Status` carries that status, so a caller can classify the failure without
  reading the message.
- `io.ErrUnexpectedEOF` - the connection ended in the middle of a message. What
  arrived is a prefix, never a whole message, and the error is sticky: further
  reads fail too. Match it with `errors.Is`.
