package websocket_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/goloop/websocket"
)

// Example runs a tiny echo server and a client against it.
func Example() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Upgrade(w, r,
			websocket.WithOriginChecker(func(*http.Request) bool { return true }))
		if err != nil {
			return
		}
		defer ws.Close()
		mt, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.WriteMessage(mt, data)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ws, _, err := websocket.Dial(context.Background(), url)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer ws.Close()

	if err := ws.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		fmt.Println("write:", err)
		return
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Println(string(data))
	// Output: ping
}
