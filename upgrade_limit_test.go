package websocket

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpgradeClampsReadLimit(t *testing.T) {
	limits := make(chan int64, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r, WithReadLimit(math.MaxInt64))
		if err != nil {
			limits <- 0
			return
		}
		defer c.Close()
		limits <- c.readLimit
	}))
	defer srv.Close()
	c, _, err := Dial(context.Background(), "ws"+srv.URL[4:])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := <-limits; got != maxReadLimit {
		t.Fatalf("limit = %d, want %d", got, maxReadLimit)
	}
}
