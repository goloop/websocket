# websocket - довідник

Власна реалізація WebSocket (RFC 6455) на стандартній бібліотеці, без сторонніх
залежностей.

## Зміст

- [Серверний upgrade](#серверний-upgrade)
- [Клієнтський dial](#клієнтський-dial)
- [Читання і запис](#читання-і-запис)
- [Control-фрейми і close](#control-фрейми-і-close)
- [Стиснення](#стиснення)
- [Ліміти і дедлайни](#ліміти-і-дедлайни)
- [Конкурентність](#конкурентність)
- [Помилки](#помилки)

## Серверний upgrade

```go
ws, err := websocket.Upgrade(w, r, opts...)          // одноразово
up := websocket.NewUpgrader(opts...); ws, err := up.Upgrade(w, r) // переюзно
```

При невдачі `Upgrade` пише HTTP-помилку і повертає `*HandshakeError`. Опції:

- `WithOriginChecker(fn)` - чи дозволений Origin запиту. Дефолт приймає
  same-origin і запити без Origin, блокуючи cross-site WebSocket hijacking.
- `WithSubprotocols(names...)` - subprotocols сервера за пріоритетом; береться
  перший, який пропонує й клієнт.
- `WithReadLimit(bytes)` - максимальний розмір отриманого повідомлення.
- `WithCompression()` / `WithCompressionLevel(level)` - permessage-deflate.
- `WithHandshakeTimeout(d)` - обмеження на запис відповіді.

Хелпери: `IsWebSocketUpgrade(r)`, `Subprotocols(r)`.

## Клієнтський dial

```go
ws, resp, err := websocket.Dial(ctx, "wss://host/path", opts...)
```

Схема - `ws` або `wss`; `wss` через TLS. Відповідь не-101 повертає
`ErrBadHandshake` разом із `*http.Response`. Опції:

- `WithDialHeader(h)` - додаткові заголовки (Authorization, Cookie, Origin).
- `WithDialSubprotocols(names...)`.
- `WithDialTLSConfig(cfg)` - TLS для `wss`.
- `WithDialNetDialer(d)` - `net.Dialer` для TCP-з'єднання.
- `WithDialCompression()` - пропонувати permessage-deflate.

## Читання і запис

Цілі повідомлення:

```go
mt, data, err := ws.ReadMessage()          // mt - TextMessage або BinaryMessage
err = ws.WriteMessage(websocket.TextMessage, data)
```

Стрімінг (нестиснені повідомлення стрімляться; стиснене спершу повністю
розпаковується):

```go
mt, r, err := ws.NextReader()   // r - io.Reader
w, err := ws.NextWriter(websocket.BinaryMessage) // w - io.WriteCloser; Close надсилає
```

JSON:

```go
err = ws.WriteJSON(v)
err = ws.ReadJSON(&v)
```

Отримане text-повідомлення валідується як UTF-8; невалідний текст закриває
з'єднання кодом `1007`.

## Control-фрейми і close

Ping, pong і close обробляє reader. На ping автоматично йде pong; close починає
closing-handshake. Перевизначити - `SetPingHandler`, `SetPongHandler`,
`SetCloseHandler`.

Надіслати власні:

```go
ws.WriteControl(websocket.PingMessage, []byte("hi"), time.Now().Add(time.Second))
ws.CloseWithStatus(websocket.CloseNormalClosure, "bye")
```

`CloseWithStatus` надсилає close-фрейм, але не закриває сокет; після close від
піра (reader поверне `*CloseError`) викличте `Close`, щоб звільнити з'єднання.
Сам `Close` закриває сокет без handshake.

## Стиснення

permessage-deflate (RFC 7692) узгоджується під час handshake, коли обидві
сторони його вмикають. Використовується «no context takeover»: кожне
повідомлення стискається незалежно. Ліміт читання застосовується до
**розпакованого** розміру - це захист від deflate-бомб.

## Ліміти і дедлайни

- `SetReadLimit(n)` обмежує одне повідомлення (дефолт 32 МіБ).
- `SetReadDeadline` / `SetWriteDeadline` обмежують I/O; ставте їх, щоб повільний
  або застряглий пір не блокував горутину.

## Конкурентність

Один reader і один writer можуть працювати паралельно. `WriteControl` безпечно
викликати з іншої горутини, ніж writer. Два одночасні reader-и чи два writer-и не
підтримуються.

## Помилки

- `*CloseError{Code, Text}` - пір закрив з'єднання. Використовуйте
  `IsCloseError(err, codes...)` і `IsUnexpectedCloseError(err, expected...)`.
- `ErrBadHandshake` - клієнтський handshake відхилено.
- `ErrCloseSent` - запис після початку closing-handshake.
- `*HandshakeError` - серверний upgrade не вдався (HTTP-помилку вже надіслано).
