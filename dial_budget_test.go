package websocket

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// silentListener accepts connections, optionally runs fn on each, and never
// completes a handshake of its own. It closes everything it accepted when the
// test ends, so a leaked client socket shows up as a failure elsewhere rather
// than as a hung test.
func silentListener(t *testing.T, fn func(net.Conn)) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			if fn != nil {
				go fn(conn)
			}
		}
	}()
	return ln
}

// The handshake timeout has to cover the TLS exchange: a peer that completes
// TCP and then says nothing must not hold the caller past it.
func TestDialTimesOutOnSilentTLSPeer(t *testing.T) {
	ln := silentListener(t, nil)

	start := time.Now()
	_, _, err := Dial(context.Background(), "wss://"+ln.Addr().String(),
		WithDialHandshakeTimeout(50*time.Millisecond),
		WithDialTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Dial succeeded against a peer that never spoke TLS")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Dial took %v, want about its 50ms handshake timeout", elapsed)
	}
}

// Cancelling the context ends the handshake at once, even with no deadline
// anywhere, and the error says it was cancelled.
func TestDialCancelEndsSilentHandshake(t *testing.T) {
	ln := silentListener(t, func(c net.Conn) {
		// Read the request and then say nothing at all.
		_, _ = bufio.NewReader(c).ReadString('\n')
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := Dial(ctx, "ws://"+ln.Addr().String(),
			WithDialHandshakeTimeout(0)) // no time bound at all
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want it to match context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dial ignored cancel() and stayed blocked")
	}
}

// A context cancelled after a successful handshake must not reach back and
// close the connection the caller was given.
func TestDialCancelAfterSuccessKeepsConn(t *testing.T) {
	srv := newRawWSServer(t, func(key string) string {
		return wsAccept(key) + "\r\n"
	})

	ctx, cancel := context.WithCancel(context.Background())
	ws, _, err := Dial(ctx, "ws://"+srv,
		WithDialHandshakeTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.Close()

	cancel()
	time.Sleep(50 * time.Millisecond)

	// The connection must still be usable: a cancelled dial context is not a
	// closed connection.
	if err := ws.WriteMessage(TextMessage, []byte("still here")); err != nil {
		t.Errorf("write after the dial context was cancelled: %v", err)
	}
}

// A server that keeps sending handshake headers is cut off by the budget
// instead of being allowed to spend the client's memory.
func TestDialRejectsOversizedHandshake(t *testing.T) {
	srv := newRawWSServer(t, func(key string) string {
		return wsAccept(key) +
			"X-Large: " + strings.Repeat("a", 256*1024) + "\r\n\r\n"
	})

	_, _, err := Dial(context.Background(), "ws://"+srv,
		WithDialHandshakeLimit(64*1024))
	if !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("err = %v, want ErrHandshakeTooLarge", err)
	}
}

// The budget must not cost correctness: a 101 that arrives in the same packet
// as the first frames still works, and those frames are not lost.
func TestDialKeepsFramesSentWithTheHandshake(t *testing.T) {
	frame := buildFrame(true, false, TextMessage, false, []byte("early"))
	srv := newRawWSServer(t, func(key string) string {
		return wsAccept(key) + "\r\n" + string(frame)
	})

	ws, _, err := Dial(context.Background(), "ws://"+srv)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer ws.Close()

	mt, body, err := ws.ReadMessage()
	if err != nil || mt != TextMessage || string(body) != "early" {
		t.Fatalf("mt=%d body=%q err=%v, want the frame sent with the handshake", mt, body, err)
	}
}

// wsAccept builds the 101 status and the protocol headers for a client key,
// without the blank line that ends the header block.
func wsAccept(key string) string {
	return "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + computeAcceptKey(key) + "\r\n"
}

// newRawWSServer serves one hand-written handshake response, built by reply
// from the client's Sec-WebSocket-Key, and returns the address to dial.
func newRawWSServer(t *testing.T, reply func(key string) string) string {
	t.Helper()
	ln := silentListener(t, func(c net.Conn) {
		br := bufio.NewReader(c)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		fmt.Fprint(c, reply(req.Header.Get("Sec-WebSocket-Key")))
		// Hold the connection open for the rest of the test.
		io := make([]byte, 1)
		_, _ = c.Read(io)
	})
	return ln.Addr().String()
}
