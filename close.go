package websocket

import (
	"encoding/binary"
	"errors"
	"strconv"
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
	CloseTLSHandshake            CloseCode = 1015
)

// ErrCloseSent is returned by a write after the closing handshake has started.
var ErrCloseSent = errors.New("websocket: close sent")

// CloseError records the close code and reason received from the peer.
type CloseError struct {
	Code CloseCode
	Text string
}

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
	case code >= 1000 && code <= 1013:
		return true
	default:
		return false
	}
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
