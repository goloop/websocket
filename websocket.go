package websocket

// MessageType identifies a WebSocket frame opcode as defined in RFC 6455
// section 5.2. The data types TextMessage and BinaryMessage are what an
// application normally sends and receives; the control types are handled by the
// connection but are also accepted by WriteControl.
type MessageType int

const (
	// TextMessage denotes a UTF-8 encoded text message.
	TextMessage MessageType = 1
	// BinaryMessage denotes a binary message.
	BinaryMessage MessageType = 2
	// CloseMessage denotes a close control message.
	CloseMessage MessageType = 8
	// PingMessage denotes a ping control message.
	PingMessage MessageType = 9
	// PongMessage denotes a pong control message.
	PongMessage MessageType = 10

	// continuationFrame is the opcode for a continuation frame of a fragmented
	// message. It is never returned to the application as a message type.
	continuationFrame MessageType = 0
)

// Frame header bit masks (RFC 6455 section 5.2).
const (
	finalBit = 1 << 7 // FIN
	rsv1Bit  = 1 << 6 // RSV1, used by permessage-deflate
	rsv2Bit  = 1 << 5
	rsv3Bit  = 1 << 4
	maskBit  = 1 << 7 // MASK, in the second header byte

	opcodeMask = 0x0f
	lengthMask = 0x7f
)

// maxControlFramePayload is the maximum payload size of a control frame.
const maxControlFramePayload = 125

// defaultReadLimit caps a single inbound message when the application does not
// set its own limit with SetReadLimit or WithReadLimit. It guards against
// unbounded memory use, including decompression bombs.
const defaultReadLimit = 32 << 20 // 32 MiB

// isControl reports whether opcode denotes a control frame.
func isControl(opcode MessageType) bool { return opcode >= CloseMessage }
