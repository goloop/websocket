[![deps.dev](https://img.shields.io/badge/deps.dev-insights-4c8dbc)](https://deps.dev/go/github.com%2Fgoloop%2Fwebsocket) [![License](https://img.shields.io/badge/license-MIT-brightgreen)](https://github.com/goloop/websocket/blob/master/LICENSE) [![License](https://img.shields.io/badge/godoc-YES-green)](https://pkg.go.dev/github.com/goloop/websocket) [![Stay with Ukraine](https://img.shields.io/static/v1?label=Stay%20with&message=Ukraine%20♥&color=ffD700&labelColor=0057B8&style=flat)](https://u24.gov.ua/)

# websocket

`websocket` implements the WebSocket protocol ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455))
on top of Go's standard library, with **no third-party dependencies**. It gives
you a server-side upgrade, a client-side dial, the permessage-deflate extension
([RFC 7692](https://www.rfc-editor.org/rfc/rfc7692)) and subprotocol negotiation.

Because the package is named `websocket`, the natural name for a connection is
`ws`:

```go
ws, err := websocket.Upgrade(w, r)
```

It composes with the rest of `net/http`, including the
[goloop/mux](https://github.com/goloop/mux) router and
[goloop/middlewares](https://github.com/goloop/middlewares) (whose `Compress`
correctly leaves upgrade requests alone).

## Features

- Server `Upgrade` (one-off or a reusable `Upgrader`) and client `Dial`.
- Text and binary messages, with `ReadMessage`/`WriteMessage` and streaming
  `NextReader`/`NextWriter`.
- Automatic ping/pong and a proper closing handshake, with overridable handlers.
- permessage-deflate compression with a decompression-bomb guard.
- Subprotocol negotiation, per-message size limits, deadlines.
- `ReadJSON`/`WriteJSON` convenience.
- **Same-origin by default**, to block cross-site WebSocket hijacking.

## Installation

```bash
go get -u github.com/goloop/websocket
```

```go
import "github.com/goloop/websocket"
```

Requires Go 1.25 or newer.

## Quick start

Server (echo):

```go
func handler(w http.ResponseWriter, r *http.Request) {
    ws, err := websocket.Upgrade(w, r)
    if err != nil {
        return // an HTTP error has already been written
    }
    defer ws.Close()
    for {
        mt, data, err := ws.ReadMessage()
        if err != nil {
            break
        }
        if err := ws.WriteMessage(mt, data); err != nil {
            break
        }
    }
}
```

Reusable configuration:

```go
up := websocket.NewUpgrader(
    websocket.WithReadLimit(1<<20),
    websocket.WithCompression(),
    websocket.WithSubprotocols("chat"),
)
ws, err := up.Upgrade(w, r)
```

Client:

```go
ws, resp, err := websocket.Dial(ctx, "wss://example.com/ws")
if err != nil {
    log.Fatal(err)
}
defer ws.Close()
ws.WriteMessage(websocket.TextMessage, []byte("hello"))
```

## Security notes

- **Origin.** The default `Upgrader` accepts only same-origin requests. To serve
  a public endpoint or a browser on another origin, pass `WithOriginChecker`
  with your own policy.
- **Message size.** `WithReadLimit` (default 32 MiB) caps a single message; the
  limit applies to the *decompressed* size, so it also bounds
  permessage-deflate.
- **Compression.** permessage-deflate uses "no context takeover" per message.
- **Deadlines.** Set `SetReadDeadline`/`SetWriteDeadline` to protect against slow
  peers.

## Concurrency

A connection supports one concurrent reader and one concurrent writer:
`ReadMessage` may run in one goroutine while `WriteMessage` runs in another.
`WriteControl` may be called concurrently with a writer. Do not run two readers
or two writers at once.

## Documentation

- Full reference and recipes: [DOC.md](DOC.md) · [DOC.UK.md](DOC.UK.md)
- Package API: [pkg.go.dev/github.com/goloop/websocket](https://pkg.go.dev/github.com/goloop/websocket)
- Changes between versions: [CHANGELOG.md](CHANGELOG.md)

## Contributing

Contributions are welcome. Please run `go test ./...`, `go test -race ./...`,
`go vet ./...` and `gofmt -l .` before submitting a pull request.

## License

`websocket` is released under the MIT License. See [LICENSE](LICENSE).
