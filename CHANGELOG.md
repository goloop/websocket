# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial v0 development branch: a from-scratch WebSocket (RFC 6455)
implementation on the standard library, with no third-party dependencies.

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
