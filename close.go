package websocket

import (
	"encoding/binary"
	"errors"
	"strconv"
	"unicode/utf8"
)

// CloseCode is a WebSocket close status code (RFC 6455 section 7.4).
type CloseCode uint16

// Close status codes defined by RFC 6455. Codes 1005, 1006 and 1015 are
// reserved and must not be sent on the wire; they only appear as locally
// generated values.
const (
	CloseNormalClosure           CloseCode = 1000
	CloseGoingAway               CloseCode = 1001
	CloseProtocolError           CloseCode = 1002
	CloseUnsupportedData         CloseCode = 1003
	CloseNoStatusReceived        CloseCode = 1005
	CloseAbnormalClosure         CloseCode = 1006
	CloseInvalidFramePayloadData CloseCode = 1007
	ClosePolicyViolation         CloseCode = 1008
	CloseMessageTooBig           CloseCode = 1009
	CloseMandatoryExtension      CloseCode = 1010
	CloseInternalServerErr       CloseCode = 1011
	CloseServiceRestart          CloseCode = 1012
	CloseTryAgainLater           CloseCode = 1013
	CloseBadGateway              CloseCode = 1014
	CloseTLSHandshake            CloseCode = 1015
)

// ErrCloseSent is returned by a write after the closing handshake has started.
var ErrCloseSent = errors.New("websocket: close sent")

// ErrWriteLimit is returned when a message would exceed the limit set by
// [Conn.SetWriteLimit]. Nothing is written and the connection stays usable,
// so a caller can send something smaller instead.
var ErrWriteLimit = errors.New("websocket: write limit exceeded")

// CloseError records how the connection ended.
//
// Usually it is the close frame the peer sent, with its code and reason. It
// is also how an abnormal end is reported: a connection that dropped without
// a closing handshake yields code 1006 with the underlying error kept as the
// cause, so errors.Is still finds io.EOF or io.ErrUnexpectedEOF beneath it.
// 1006 is a local observation and is never put on the wire.
type CloseError struct {
	Code CloseCode
	Text string

	// cause is the error the connection actually failed with, for an
	// abnormal close. It is nil for a close frame from the peer.
	cause error
}

// Unwrap returns the error behind an abnormal close, so a caller can still
// match io.EOF or io.ErrUnexpectedEOF through it.
func (e *CloseError) Unwrap() error { return e.cause }

// Error implements the error interface, formatting the close code and, when
// present, the reason text the peer sent with the close frame.
func (e *CloseError) Error() string {
	s := "websocket: close " + strconv.Itoa(int(e.Code))
	if e.Text != "" {
		s += " (" + e.Text + ")"
	}
	return s
}

// IsCloseError reports whether err is a *CloseError with one of the given codes.
func IsCloseError(err error, codes ...CloseCode) bool {
	var ce *CloseError
	if errors.As(err, &ce) {
		for _, c := range codes {
			if ce.Code == c {
				return true
			}
		}
	}
	return false
}

// IsUnexpectedCloseError reports whether err is a *CloseError whose code is not
// one of the expected ones. It is the usual way to tell a clean shutdown from a
// surprising one in a read loop.
func IsUnexpectedCloseError(err error, expected ...CloseCode) bool {
	var ce *CloseError
	if errors.As(err, &ce) {
		for _, c := range expected {
			if ce.Code == c {
				return false
			}
		}
		return true
	}
	return false
}

// isValidReceivedCloseCode reports whether code is allowed in a close frame
// received from a peer.
func isValidReceivedCloseCode(code CloseCode) bool {
	switch {
	case code >= 3000 && code <= 4999:
		return true // registered/private use ranges
	case code == 1004,
		code == CloseNoStatusReceived,
		code == CloseAbnormalClosure,
		code == CloseTLSHandshake:
		return false // reserved, must not appear on the wire
	case code >= 1000 && code <= 1014:
		return true
	default:
		return false
	}
}

// isValidSentCloseCode reports whether code may be put on the wire. The three
// reserved codes are the ones a program only ever observes locally: they
// describe how a connection ended, not something a peer can be told.
func isValidSentCloseCode(code CloseCode) bool {
	return isValidReceivedCloseCode(code)
}

// validClosebytes reports whether a close payload is one this connection may
// send: empty, or a code that may go on the wire followed by a UTF-8 reason,
// within the control-frame limit.
func validClosePayload(payload []byte) bool {
	switch {
	case len(payload) == 0:
		return true
	case len(payload) == 1:
		return false // a code is two bytes or nothing at all
	case len(payload) > maxControlFramePayload:
		return false
	}
	if !isValidSentCloseCode(CloseCode(binary.BigEndian.Uint16(payload[:2]))) {
		return false
	}
	return utf8.Valid(payload[2:])
}

// formatCloseMessage builds a close frame payload: a two-byte big-endian code
// followed by the optional UTF-8 reason. A zero code produces an empty payload,
// meaning "no status".
func formatCloseMessage(code CloseCode, text string) []byte {
	if code == 0 {
		return []byte{}
	}
	buf := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(buf, uint16(code))
	copy(buf[2:], text)
	return buf
}
