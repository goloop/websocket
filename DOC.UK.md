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

Заголовки, які handler виставив на `ResponseWriter` до upgrade (насамперед
session-cookie), переносяться у `101`; власні заголовки протоколу з writer-а не
беруться, а заголовок, чиї ім'я чи значення могли б розірвати відповідь,
завершує upgrade з `500`.

- `WithOriginChecker(fn)` - чи дозволений Origin запиту. `nil` повертає
  дефолт. Дефолт вимагає рівно один коректний `Origin`, чий host збігається з
  `Host`, відхиляє непрозорий (`null`) origin і схему, відмінну від http/https,
  а якщо запит прийшов напряму через TLS - вимагає https. Порти порівнюються
  лише коли їх вказано з обох боків: за reverse proxy сервер не бачить ані
  зовнішньої схеми, ані зовнішнього порту; кому потрібен їх облік - передає сюди
  власний allowlist. Запит без Origin (типовий не-браузерний клієнт) приймається.
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
- `WithDialHandshakeTimeout(d)` - обмежує TLS-handshake, запит і відповідь
  (дефолт 45 с).
- `WithDialHandshakeLimit(n)` - обмежує обсяг прочитаної відповіді handshake
  (дефолт 64 КіБ), інакше `ErrHandshakeTooLarge`.

Щойно TCP-з'єднання встановлено, весь handshake іде під одним бюджетом: це
раніший із дедлайну контексту й handshake-таймауту, тож довгий контекст не
підмінює короткий таймаут. Скасування контексту завершує handshake одразу, а не
за дедлайном, і помилка тоді збігається з `context.Canceled`. Уже повернене
з'єднання належить викликачу: скасований пізніше контекст його не закриває.

## Читання і запис

Цілі повідомлення:

```go
mt, data, err := ws.ReadMessage()          // mt - TextMessage або BinaryMessage
err = ws.WriteMessage(websocket.TextMessage, data)
```

Читання стрімиться (стиснене повідомлення спершу повністю розпаковується),
запис - ні: `NextWriter` накопичує ціле повідомлення в пам'яті й надсилає його
на `Close`.

```go
mt, r, err := ws.NextReader()   // r - io.Reader
w, err := ws.NextWriter(websocket.BinaryMessage) // w - io.WriteCloser; Close надсилає
```

Через це буферування `io.Copy` у такий writer із необмеженого джерела обмежений
лише пам'яттю. `SetWriteLimit` перетворює це на помилку, яку можна обробити.

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

- `SetReadLimit(n)` обмежує одне повідомлення (дефолт 32 МіБ). Обмеження
  стосується всього повідомлення, включно з частиною, пропущеною через
  відкриття наступного reader-а.
- `SetWriteLimit(n)` обмежує одне вихідне повідомлення, інакше `ErrWriteLimit`.
  Дефолт 0 - без обмеження.
- `SetReadDeadline` / `SetWriteDeadline` обмежують I/O; ставте їх, щоб повільний
  або застряглий пір не блокував горутину. `SetWriteDeadline` також завершує
  вже розпочатий запис, як на `net.Conn`, тож його можна викликати з іншої
  горутини, щоб зняти застряглий.
- Дедлайн, переданий у `WriteControl`, обмежує весь виклик, включно з
  очікуванням на запис даних, що триває. Якщо він настає раніше, кадр не
  надсилається, а помилка повертає `Timeout() == true`. Він керує саме цим
  викликом: заміщає дедлайн, який з'єднання вже мало, а паралельний
  `SetWriteDeadline` може його скоротити, але не зняти.

## Конкурентність

Один reader і один writer можуть працювати паралельно. `WriteControl` безпечно
викликати з іншої горутини, ніж writer. Два одночасні reader-и чи два writer-и не
підтримуються.

## Помилки

- `*CloseError{Code, Text}` - пір закрив з'єднання. Використовуйте
  `IsCloseError(err, codes...)` і `IsUnexpectedCloseError(err, expected...)`.
- `ErrBadHandshake` - клієнтський handshake відхилено.
- `ErrHandshakeTooLarge` - відповідь handshake перевищила байтовий бюджет.
- `ErrCloseSent` - запис після початку closing-handshake.
- `ErrWriteLimit` - повідомлення перевищило межу `SetWriteLimit`. Нічого не
  надіслано, з'єднання лишається робочим.
- `*HandshakeError` - серверний upgrade не вдався (HTTP-помилку вже надіслано).
  Поле `Status` несе цей статус, тож помилку можна класифікувати без розбору
  тексту.
- `io.ErrUnexpectedEOF` - з'єднання обірвалося посеред повідомлення. Те, що
  надійшло, є префіксом, а не цілим повідомленням; помилка липка, тож наступні
  читання теж не вдадуться. Перевіряйте через `errors.Is`.
