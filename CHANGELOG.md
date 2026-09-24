# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-09-24

Minor release: configuration that is checked rather than guessed, and
compression that engages with the browsers that ask for it.

### Fixed
- A `permessage-deflate` offer carrying `client_max_window_bits` is accepted.
  The server declined the whole offer over that parameter, so a browser that
  sends it, which is the common case, got no compression at all. The parameter
  is accepted and not echoed: without a value it only says the server may cap
  the client's window, which this server does not do, and with a value it
  promises a window this server's own full-size window can always decompress.
  `server_max_window_bits`, which would require shrinking this server's
  window, is still declined.
- A `permessage-deflate` offer with a repeated parameter, or a value on a flag
  that takes none, is declined instead of accepted.
- Options copy what the caller still owns. `WithSubprotocols`,
  `WithDialSubprotocols`, `WithDialHeader` and `WithDialTLSConfig` kept the
  caller's slice, header or config, so a later change reached into a
  connection that was already configured.

### Added
- `ErrConfig` and configuration checking. A compression level outside the
  flate range, or a subprotocol that is not an HTTP token, now fails: `Dial`
  returns `ErrConfig` before touching the network, and `Upgrade` answers 500
  on the first request. An out-of-range level used to be replaced by the
  default, which hid the mistake behind working compression.
- `WithDialReadLimit` and `WithDialCompressionLevel`, the client-side
  counterparts of the server options of the same name.
- `Conn.CompressionEnabled` reports whether permessage-deflate was negotiated.
  Asking for it and getting it were indistinguishable before.

## [0.5.0] - 2026-09-24

Minor release: errors a caller can act on. A connection that drops without a
closing handshake now reports a `*CloseError` where it used to report a bare
`io.EOF`; `errors.Is(err, io.EOF)` still matches, a bare `err == io.EOF` does
not.

### Fixed
- An abrupt end of the connection is reported as a `*CloseError` with code
  1006, the way every other ending already was. It used to surface as a raw
  `io.EOF`, which `IsUnexpectedCloseError` does not recognise, so the one
  helper meant for telling a clean shutdown from a surprising one stayed
  silent for the most surprising case of all. The original error is kept as
  the cause, so `errors.Is(err, io.EOF)` and `errors.Is(err,
  io.ErrUnexpectedEOF)` keep working, and 1006 is still never put on the wire.
- A close handler's error reaches the caller. `SetCloseHandler` documents a
  handler that returns an error, and the reader discarded it, so a handler
  that failed had no way to report it.

### Added
- Exported errors for the conditions worth branching on: `ErrReadLimit` for a
  message past the limit, `ErrProtocol` for any framing violation by the peer,
  and `ErrWriteClosed`, `ErrBadControl`, `ErrControlTooBig`,
  `ErrBadWriteType`, `ErrBadClosePayload` for a call whose arguments were
  wrong. Until now these were unexported, so the only way to tell them apart
  was to match on the message text.
- `CloseError.Unwrap` returns the error behind an abnormal close.

## [0.4.0] - 2026-09-23

Minor release: the protocol-contract and API fixes from the audit. Two changes
are visible to existing code: `Upgrade` now returns a `*HandshakeError`, and
the default origin policy is stricter.

### Fixed
- `Upgrade` keeps the headers the handler set before calling it. The `101` is
  written by hand and ignored `w.Header()` entirely, so a session cookie set
  with `http.SetCookie`, a trace header or anything a middleware added was
  dropped while the upgrade reported success. They are carried over now,
  except the protocol's own headers, which the handshake still controls. A
  header whose name or value could split the response fails the upgrade
  instead of being written.
- `WriteControl` accepts only Close, Ping and Pong. It tested for "any opcode
  from 8 up", and the frame writer masks an opcode to four bits, so
  `WriteControl(24, ...)` put a close frame on the wire while the connection
  went on believing no close had been sent.
- A close frame is checked before it is sent: the code must be one that may
  appear on the wire, which rules out 1005, 1006 and 1015, and the reason must
  be valid UTF-8 and fit a control frame. A bad argument is refused before the
  write lock is taken, so it neither writes bytes nor changes the connection's
  state.
- The client validates every extension the server selected, not only the one
  it hoped for. An extension that was never offered was accepted and ignored,
  as were a repeated `permessage-deflate`, a repeated parameter and a value on
  a flag that takes none. A duplicated `Sec-WebSocket-Accept` or
  `Sec-WebSocket-Protocol` is refused too, rather than read as its first
  value.
- Close code 1014 (Bad Gateway) is a registered code and is accepted from a
  peer. `CloseBadGateway` names it.
- A zero `Upgrader`, and `WithOriginChecker(nil)`, no longer panic on the
  first valid request: both mean the default same-origin policy.
- `SetReadLimit` no longer wraps around. A limit near `math.MaxInt64`
  overflowed the derived compressed bound and made the connection reject every
  message, including a five-byte one. The limit is clamped to the largest
  value that keeps the arithmetic positive.

### Added
- `HandshakeError.Status` is the HTTP status the failed upgrade wrote, so a
  caller can classify a failure without matching on the message.
- `CloseBadGateway` (1014).

### Changed
- `Upgrade` returns `*HandshakeError` rather than `HandshakeError`. The
  reference always described a pointer, and `errors.As(err, &he)` with a
  `*HandshakeError`, which is what a caller writes, never matched. Code that
  type-asserted the value form has to take the pointer instead.
- The default origin policy is stricter. It used to compare only the host of
  the first `Origin` header, so an `http://` page was accepted by an endpoint
  served over TLS, and a value with a path, a foreign scheme, or a second
  `Origin` header went through. It now requires exactly one well-formed
  origin, an http or https scheme, https when the request arrived over TLS
  directly, and refuses an opaque `null` origin. Ports are compared only when
  both sides state one, so a deployment behind a TLS-terminating proxy is
  unaffected.

## [0.3.0] - 2026-09-23

Minor release: the limit and memory fixes from the audit. The read limit now
means what it says, which is stricter than before on messages that were only
partly read.

### Fixed
- The read limit covers a whole message, including the part skipped when the
  next reader is opened. Discarding the rest of a message built a fresh source
  with a counter of its own, so a message could be read up to the limit and
  then skipped up to the limit again, letting roughly twice the budget through
  for one message.
- A text message is checked as UTF-8 in the part that was skipped too. The
  discard did not carry the reader's UTF-8 state, so invalid bytes in the
  skipped remainder were never reported.
- An oversized message no longer hands its bytes to the caller along with the
  error. `Read` returned the whole chunk it had read together with the
  limit error, and an io.Reader consumer is entitled to use bytes it was
  given, so `io.Copy` passed them downstream. Nothing past the limit is
  returned now. A message exactly the size of the limit is still accepted.
- Pooled compressors let go of the message they worked on. A flate writer went
  back to the pool still pointing at the compressed output, and a flate reader
  still pointing at the payload of a message that failed to inflate, keeping
  buffers of the message's own size reachable for as long as the pool held the
  object.
- A closed `NextWriter` releases its buffer. A writer kept by the application
  after `Close` held the whole message, and the connection with it.
- `ReadMessage` no longer copies a compressed message a second time. It was
  inflated into one buffer and then copied again through `io.ReadAll`, which
  doubled the peak memory of every compressed message.

### Added
- `SetWriteLimit(n)` caps a single outgoing message, failing with the new
  `ErrWriteLimit` before anything is buffered or sent. The default, 0, means
  no cap, so nothing changes for a caller that does not set one.

### Changed
- The reference no longer calls `NextWriter` streaming. It buffers the whole
  message and sends it on `Close`, which is what it always did; only the
  wording was wrong.

## [0.2.0] - 2026-09-23

Minor release: the integrity and lifetime fixes from the audit. No function
was removed or renamed, but a truncated message now fails where it used to
succeed, so read the first entry before upgrading.

### Fixed
- A message whose last frame is not final is no longer returned as a whole
  message. When the connection ended while the peer still owed a continuation
  frame, the reader reported `io.EOF`, which `io.ReadAll` and `ReadMessage`
  both read as a clean end of message: the caller got the prefix that had
  arrived as if it were complete. A plain transport drop was enough to cause
  it, and a prefix can be a valid command or JSON document on its own. Such a
  read now fails with an error matching `io.ErrUnexpectedEOF`, and the failure
  is sticky, so a later read cannot succeed either. The same applies while
  discarding an unread message and to compressed messages.
- `Dial` bounds the TLS handshake. The deadline was set only after TLS was
  negotiated, so a peer that completed the TCP connection and then said
  nothing held the caller's goroutine and socket indefinitely.
- `Dial` answers context cancellation during the handshake. Only an absolute
  `ctx.Deadline()` was carried over to the socket, so `cancel()` did nothing
  until that deadline, and a context without one never interrupted the
  handshake at all. The handshake now ends as soon as the context does, with
  an error matching `context.Canceled` or `context.DeadlineExceeded`. A
  connection already handed back to the caller is never closed by a late
  cancellation.
- `Dial` bounds the size of the handshake response. No limit applied to it
  (`http.Transport`, which would impose one, is not involved), so a server
  could send headers until the client ran out of memory. The default budget is
  64 KiB, and passing it fails with the new `ErrHandshakeTooLarge`.
- A `SetWriteDeadline` from another goroutine can no longer lift the deadline
  of a control write in flight. It cleared the socket deadline that
  `WriteControl` had set for itself, leaving the close or pong the reader was
  sending blocked on a peer that had stopped reading. Shortening a deadline
  still takes effect; only lengthening or clearing it is refused for the
  duration of that call.

### Added
- `WithDialHandshakeLimit(bytes)` sets the handshake response budget, and
  `ErrHandshakeTooLarge` reports that it was passed. A value <= 0 removes the
  bound.

### Changed
- `WithDialHandshakeTimeout` now bounds the TLS handshake as well as the
  request and the response, and when the context carries a deadline of its own
  the earlier of the two applies. It used to be ignored entirely whenever the
  context had any deadline, so a long-lived context silently replaced a short
  timeout.

## [0.1.3] - 2026-09-21

### Fixed
- A data write stuck on a peer that stopped reading no longer holds the
  connection's control frames and deadlines hostage. `WriteControl` waited for
  the write lock with no bound and applied its deadline only afterwards, and
  `SetWriteDeadline` took the same lock, so with no deadline set beforehand
  the ping, pong and close frames the reader sends, and the very call meant
  to end the stuck write, all hung until `Close`. The deadline given to
  `WriteControl` now bounds the wait as well and reports a timeout when it
  passes first, and `SetWriteDeadline` no longer waits for the lock, so it
  ends a write in progress exactly as it does on a `net.Conn`.

## [0.1.2] - 2026-07-11

### Fixed
- `Upgrade` now hijacks the connection through `http.ResponseController`
  instead of a direct `w.(http.Hijacker)` assertion. The upgrade therefore
  works behind middleware that wraps the `http.ResponseWriter` and exposes
  only `Unwrap` (the standard-library-idiomatic pattern since Go 1.20), not a
  direct `Hijacker`. Writers that already expose `Hijacker` are unaffected.

## [0.1.1] - 2026-07-10

### Documentation
- Documented the exported `CloseError.Error` and `HandshakeError.Error`
  methods, plus the internal frame/message reader and writer methods.

## [0.1.0]

Initial v0 release: a from-scratch WebSocket (RFC 6455) implementation on the
standard library, with no third-party dependencies.

### Added

- Server `Upgrade` and reusable `Upgrader` (`NewUpgrader`), with same-origin
  default, subprotocol negotiation and options.
- Client `Dial` over `ws`/`wss`, with TLS, custom headers and subprotocols.
- `Conn` with `ReadMessage`/`WriteMessage`, streaming `NextReader`/`NextWriter`,
  and `WriteControl`.
- Automatic ping/pong and closing handshake, with `SetPingHandler`,
  `SetPongHandler`, `SetCloseHandler`.
- permessage-deflate (RFC 7692) with "no context takeover" and a
  decompression-bomb guard.
- Per-message read limit and connection deadlines.
- `ReadJSON`/`WriteJSON`.
- Close codes, `CloseError`, `IsCloseError`, `IsUnexpectedCloseError`.
- Safe masking without package `unsafe`.
- `WithDialHandshakeTimeout` to bound the client handshake when the context
  carries no deadline (now bounded by default).

### Fixed

- The connection's own control writes (auto-pong, close echo) no longer leave a
  stale write deadline on the socket, which could permanently kill later writes
  a few seconds after a ping. `WriteMessage` on a control type no longer clears
  a deadline the caller set.
- Reserved opcodes 0xB-0xF now fail the connection with 1002 instead of being
  silently swallowed.
- The streaming `NextReader` validates UTF-8 for text messages, including runes
  split across frame or read boundaries, failing with 1007 like `ReadMessage`.
- The client validates the negotiated permessage-deflate parameters and the
  selected subprotocol, failing the handshake on a response it cannot honour.
- The per-message read limit is enforced while discarding an unread message, so
  a peer cannot force an unbounded discard.
- The read limit applies to the decompressed size, so a legal incompressible
  message no longer trips 1009 because deflate expanded it slightly.
- A close frame with the reserved code 1004 is rejected as a protocol error.
- The server validates the `Sec-WebSocket-Key` format; `Subprotocols` reads
  every `Sec-WebSocket-Protocol` field line.
