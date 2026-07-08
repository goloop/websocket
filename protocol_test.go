package websocket

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"
)

// buildFrame assembles a raw WebSocket frame for tests, so malformed frames can
// be sent that the normal writer would never produce.
func buildFrame(fin, rsv1 bool, opcode MessageType, masked bool, payload []byte) []byte {
	var b []byte
	b0 := byte(opcode) & 0x0f
	if fin {
		b0 |= 0x80
	}
	if rsv1 {
		b0 |= 0x40
	}
	b = append(b, b0)

	var mb byte
	if masked {
		mb = 0x80
	}
	n := len(payload)
	switch {
	case n < 126:
		b = append(b, mb|byte(n))
	case n < 1<<16:
		b = append(b, mb|126, byte(n>>8), byte(n))
	default:
		b = append(b, mb|127)
		for i := 7; i >= 0; i-- {
			b = append(b, byte(n>>(8*i)))
		}
	}

	if masked {
		key := [4]byte{0xaa, 0xbb, 0xcc, 0xdd}
		b = append(b, key[:]...)
		p := append([]byte(nil), payload...)
		for i := range p {
			p[i] ^= key[i&3]
		}
		b = append(b, p...)
	} else {
		b = append(b, payload...)
	}
	return b
}

func TestProtocolViolationsClose(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
		code  CloseCode
	}{
		{"unmasked client frame", buildFrame(true, false, TextMessage, false, []byte("hi")), CloseProtocolError},
		{"reserved bit set", buildFrame(true, true, TextMessage, true, []byte("hi")), CloseProtocolError},
		{"unknown opcode", buildFrame(true, false, MessageType(3), true, []byte("hi")), CloseProtocolError},
		{"reserved opcode 0xB", buildFrame(true, false, MessageType(0x0B), true, []byte("hi")), CloseProtocolError},
		{"fragmented control", buildFrame(false, false, PingMessage, true, []byte("hi")), CloseProtocolError},
		{"oversize control", buildFrame(true, false, PingMessage, true, bytes.Repeat([]byte("a"), 126)), CloseProtocolError},
		{"invalid utf8 text", buildFrame(true, false, TextMessage, true, []byte{0xff, 0xfe}), CloseInvalidFramePayloadData},
		{"invalid utf8 split across frames",
			append(buildFrame(false, false, TextMessage, true, []byte{0xc3}),
				buildFrame(true, false, continuationFrame, true, []byte{0x28})...),
			CloseInvalidFramePayloadData},
		{"reserved close code 1004", buildFrame(true, false, CloseMessage, true, []byte{0x03, 0xec}), CloseProtocolError},
		{"reserved close code 1005", buildFrame(true, false, CloseMessage, true, []byte{0x03, 0xed}), CloseProtocolError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(echoHandler())
			defer srv.Close()
			ws := dialEcho(t, srv)
			defer ws.Close()

			if _, err := ws.NetConn().Write(tc.frame); err != nil {
				t.Fatalf("write raw frame: %v", err)
			}
			_, _, err := ws.ReadMessage()
			if !IsCloseError(err, tc.code) {
				t.Fatalf("want close %d, got %v", tc.code, err)
			}
		})
	}
}

// TestValidUTF8SplitAcrossFrames checks that a multi-byte rune split at a frame
// boundary is accepted, exercising the incremental streaming validator.
func TestValidUTF8SplitAcrossFrames(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	// "é" is 0xC3 0xA9; send the two bytes in separate fragments.
	frame := append(buildFrame(false, false, TextMessage, true, []byte{0xc3}),
		buildFrame(true, false, continuationFrame, true, []byte{0xa9})...)
	if _, err := ws.NetConn().Write(frame); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
	mt, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != TextMessage || string(data) != "é" {
		t.Fatalf("echo mismatch: type %d body %q", mt, data)
	}
}

// TestUTF8Validator covers the incremental validator directly, including runes
// split across write calls and a trailing incomplete rune.
func TestUTF8Validator(t *testing.T) {
	t.Run("valid split", func(t *testing.T) {
		var v utf8Validator
		if err := v.write([]byte{0xc3}); err != nil { // first byte of "é"
			t.Fatalf("carry: %v", err)
		}
		if err := v.write([]byte{0xa9, 'z'}); err != nil {
			t.Fatalf("complete: %v", err)
		}
		if err := v.done(); err != nil {
			t.Fatalf("done: %v", err)
		}
	})
	t.Run("invalid continuation", func(t *testing.T) {
		var v utf8Validator
		_ = v.write([]byte{0xc3})
		if err := v.write([]byte{0x28}); err == nil {
			t.Fatal("want error on invalid continuation byte")
		}
	})
	t.Run("lone continuation byte", func(t *testing.T) {
		var v utf8Validator
		if err := v.write([]byte{0x80}); err == nil {
			t.Fatal("want error on a lone continuation byte")
		}
	})
	t.Run("incomplete trailing rune", func(t *testing.T) {
		var v utf8Validator
		if err := v.write([]byte{'a', 0xc3}); err != nil {
			t.Fatalf("carry: %v", err)
		}
		if err := v.done(); err == nil {
			t.Fatal("want error on an incomplete trailing rune")
		}
	})
}

// TestWriteDeadlineRestored checks that a control write with an explicit
// deadline (auto-pong style) restores the user's deadline afterwards, and that
// WriteMessage on a control type does not clear a deadline the user set.
func TestWriteDeadlineRestored(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	// User sets a deadline in the past: subsequent writes must time out.
	if err := ws.SetWriteDeadline(time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	// A control write with its own future deadline succeeds, then must restore
	// the user's (past) deadline.
	if err := ws.WriteControl(PongMessage, nil, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("control write with future deadline: %v", err)
	}
	if err := ws.WriteMessage(TextMessage, []byte("hi")); err == nil {
		t.Fatal("restored past deadline should fail the write")
	}

	// WriteMessage of a control type passes a zero deadline and must not clear
	// the user's deadline, so it still times out.
	if err := ws.WriteMessage(PingMessage, nil); err == nil {
		t.Fatal("WriteMessage(ping) must honour the existing past deadline")
	}
}

func TestCleanCloseFromPeer(t *testing.T) {
	// A well-formed close from the peer surfaces as a *CloseError with the code.
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	ws.NetConn().Write(buildFrame(true, false, CloseMessage, true,
		[]byte{0x03, 0xe9, 'b', 'y', 'e'})) // 1001 going away + "bye"
	_, _, err := ws.ReadMessage()
	if !IsCloseError(err, CloseGoingAway) {
		t.Fatalf("want close 1001, got %v", err)
	}
	var ce *CloseError
	if IsUnexpectedCloseError(err, CloseGoingAway) {
		t.Fatal("1001 should be expected here")
	}
	_ = ce
}
