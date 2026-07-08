# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
