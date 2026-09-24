package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"
)

// A connection that ends without a closing handshake is reported the way
// every other ending is: as a *CloseError, with code 1006 and the underlying
// error still reachable.
func TestAbnormalCloseIsACloseError(t *testing.T) {
	c := newFuzzConn(nil) // no frames at all: the peer simply went away

	_, _, err := c.ReadMessage()
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %#v, want a *CloseError", err)
	}
	if ce.Code != CloseAbnormalClosure {
		t.Errorf("Code = %d, want %d", ce.Code, CloseAbnormalClosure)
	}
	if !errors.Is(err, io.EOF) {
		t.Error("the cause is no longer reachable with errors.Is")
	}
	if !IsUnexpectedCloseError(err, CloseNormalClosure, CloseGoingAway) {
		t.Error("IsUnexpectedCloseError does not recognise an abrupt drop")
	}
}

// A message cut off between fragments is an abnormal close too, and the
// truncation error documented in 0.2.0 stays reachable through it.
func TestTruncatedMessageIsAnAbnormalClose(t *testing.T) {
	c := newFuzzConn(buildFrame(false, false, BinaryMessage, false, []byte("partial")))

	_, _, err := c.ReadMessage()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want it to still match io.ErrUnexpectedEOF", err)
	}
	var ce *CloseError
	if !errors.As(err, &ce) || ce.Code != CloseAbnormalClosure {
		t.Errorf("err = %#v, want a 1006 *CloseError", err)
	}
}

// A close frame from the peer keeps saying what the peer said.
func TestPeerCloseKeepsItsCode(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, CloseMessage, false,
		[]byte{0x03, 0xe9, 'b', 'y', 'e'}))

	_, _, err := c.ReadMessage()
	if !IsCloseError(err, CloseGoingAway) {
		t.Fatalf("err = %v, want close 1001", err)
	}
	var ce *CloseError
	if errors.As(err, &ce) && ce.Unwrap() != nil {
		t.Error("a close frame should carry no underlying cause")
	}
}

// Every framing violation matches one sentinel, so a read loop can tell a
// misbehaving peer from a message that was merely too big.
func TestProtocolViolationsShareASentinel(t *testing.T) {
	cases := map[string][]byte{
		"reserved bits set":    {0xb1, 0x00},
		"unknown opcode":       {0x83, 0x00},
		"fragmented control":   {0x09, 0x00},
		"unmasked client data": buildFrame(true, false, TextMessage, false, []byte("x")),
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			c := newConn(nopConn{}, true, bufio.NewReader(bytes.NewReader(wire)), "", false, 0)
			_, _, err := c.ReadMessage()
			if !errors.Is(err, ErrProtocol) {
				t.Errorf("err = %v, want it to match ErrProtocol", err)
			}
			if errors.Is(err, ErrReadLimit) {
				t.Error("a protocol error must not look like a limit error")
			}
		})
	}
}

// The limit error is its own sentinel and is exported for the same reason.
func TestReadLimitHasItsOwnSentinel(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, BinaryMessage, false, make([]byte, 64)))
	c.SetReadLimit(16)

	_, _, err := c.ReadMessage()
	if !errors.Is(err, ErrReadLimit) {
		t.Errorf("err = %v, want ErrReadLimit", err)
	}
	if errors.Is(err, ErrProtocol) {
		t.Error("a limit error must not look like a protocol error")
	}
}

// A close handler that fails has something to say, and the reader says it.
func TestCloseHandlerErrorReachesTheCaller(t *testing.T) {
	srv := httptest.NewServer(echoHandler())
	defer srv.Close()
	ws := dialEcho(t, srv)
	defer ws.Close()

	sentinel := errors.New("handler said no")
	ws.SetCloseHandler(func(CloseCode, string) error { return sentinel })

	ws.NetConn().Write(buildFrame(true, false, CloseMessage, true,
		[]byte{0x03, 0xe8}))
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))

	if _, _, err := ws.ReadMessage(); !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the handler's own error", err)
	}
}
