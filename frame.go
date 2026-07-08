package websocket

import (
	"encoding/binary"
	"io"
)

// readFrameHeader reads and validates the next frame header, then reads the
// masking key if present. On success the read-frame state (readRemaining,
// readFinal, readMasked, readMaskKey) is set and the payload is ready to be
// read. It returns the opcode and whether RSV1 (compression) is set.
func (c *Conn) readFrameHeader() (opcode MessageType, compressed bool, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	b0, b1 := h[0], h[1]

	if b0&(rsv2Bit|rsv3Bit) != 0 {
		return 0, false, protocolError("reserved bits set")
	}
	fin := b0&finalBit != 0
	compressed = b0&rsv1Bit != 0
	opcode = MessageType(b0 & opcodeMask)
	masked := b1&maskBit != 0
	length := int64(b1 & lengthMask)

	switch length {
	case 126:
		var e [2]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint16(e[:]))
		if length < 126 {
			return 0, false, protocolError("length not minimally encoded")
		}
	case 127:
		var e [8]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		u := binary.BigEndian.Uint64(e[:])
		if u > 1<<63-1 {
			return 0, false, protocolError("frame too large")
		}
		length = int64(u)
		if length < 1<<16 {
			return 0, false, protocolError("length not minimally encoded")
		}
	}

	if c.isServer && !masked {
		return 0, false, protocolError("client frame is not masked")
	}
	if !c.isServer && masked {
		return 0, false, protocolError("server frame is masked")
	}

	if isControl(opcode) {
		if opcode != CloseMessage && opcode != PingMessage && opcode != PongMessage {
			return 0, false, protocolError("unknown opcode")
		}
		if !fin {
			return 0, false, protocolError("fragmented control frame")
		}
		if length > maxControlFramePayload {
			return 0, false, protocolError("control frame too large")
		}
		if compressed {
			return 0, false, protocolError("compressed control frame")
		}
	} else if opcode != continuationFrame && opcode != TextMessage && opcode != BinaryMessage {
		return 0, false, protocolError("unknown opcode")
	}

	if masked {
		if _, err = io.ReadFull(c.br, c.readMaskKey[:]); err != nil {
			return
		}
	}

	c.readMasked = masked
	c.readMaskPos = 0
	c.readRemaining = length
	c.readFinal = fin
	return opcode, compressed, nil
}

// writeFrameLocked writes a complete frame. The caller must hold writeMu. As a
// client the frame is masked with a fresh key; as a server it is not. The
// caller's payload is never modified.
func (c *Conn) writeFrameLocked(opcode MessageType, fin, compressed bool, payload []byte) error {
	if c.writeErr != nil {
		return c.writeErr
	}

	n := len(payload)
	headerSize := 2
	switch {
	case n >= 1<<16:
		headerSize = 10
	case n >= 126:
		headerSize = 4
	}
	if !c.isServer {
		headerSize += 4 // masking key
	}

	buf := make([]byte, headerSize+n)

	b0 := byte(opcode) & opcodeMask
	if fin {
		b0 |= finalBit
	}
	if compressed {
		b0 |= rsv1Bit
	}
	buf[0] = b0

	var mb byte
	if !c.isServer {
		mb = maskBit
	}
	switch {
	case n < 126:
		buf[1] = mb | byte(n)
	case n < 1<<16:
		buf[1] = mb | 126
		binary.BigEndian.PutUint16(buf[2:4], uint16(n))
	default:
		buf[1] = mb | 127
		binary.BigEndian.PutUint64(buf[2:10], uint64(n))
	}

	if c.isServer {
		copy(buf[headerSize:], payload)
	} else {
		key := newMaskKey()
		copy(buf[headerSize-4:headerSize], key[:])
		copy(buf[headerSize:], payload)
		maskBytes(key, 0, buf[headerSize:])
	}

	if _, err := c.conn.Write(buf); err != nil {
		return c.setWriteErr(err)
	}
	return nil
}
