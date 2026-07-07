package websocket

import (
	"bytes"
	"net/http/httptest"
	"testing"
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
		{"fragmented control", buildFrame(false, false, PingMessage, true, []byte("hi")), CloseProtocolError},
		{"oversize control", buildFrame(true, false, PingMessage, true, bytes.Repeat([]byte("a"), 126)), CloseProtocolError},
		{"invalid utf8 text", buildFrame(true, false, TextMessage, true, []byte{0xff, 0xfe}), CloseInvalidFramePayloadData},
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
