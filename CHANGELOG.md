# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
