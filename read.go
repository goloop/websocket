package websocket

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
	"unicode/utf8"
)

// ReadMessage reads the next complete data message. It transparently handles
// interleaved control frames (answering pings, tracking pongs and closes) and,
// if negotiated, decompresses the message. A text message is validated as UTF-8.
func (c *Conn) ReadMessage() (MessageType, []byte, error) {
	mt, r, err := c.NextReader()
	if err != nil {
		return 0, nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, nil, err
	}
	if mt == TextMessage && !utf8.Valid(data) {
		return 0, nil, c.failClose(CloseInvalidFramePayloadData,
			protocolError("invalid UTF-8 in text message"))
	}
	return mt, data, nil
}

// NextReader returns a reader for the next data message. Only one reader may be
// active at a time; opening a new one discards any unread bytes of the previous
// message. A compressed message is fully inflated and returned as an in-memory
// reader; an uncompressed one is streamed.
func (c *Conn) NextReader() (MessageType, io.Reader, error) {
	if c.readErr != nil {
		return 0, nil, c.readErr
	}
	if c.inMessage {
		if err := c.drainMessage(); err != nil {
			return 0, nil, c.abort(err)
		}
	}

	opcode, compressed, err := c.nextDataFrame()
	if err != nil {
		return 0, nil, c.abort(err)
	}
	if opcode == continuationFrame {
		return 0, nil, c.abort(protocolError("unexpected continuation frame"))
	}
	if compressed && !c.readDecomp {
		return 0, nil, c.abort(protocolError("unexpected compressed frame"))
	}

	c.inMessage = true
	c.readMsgType = opcode
	c.readLength = 0

	src := &frameSource{c: c}
	if !compressed {
		return c.readMsgType, &messageReader{c: c, src: src}, nil
	}

	// Compressed: read the whole (bounded) compressed message, then inflate with
	// the read limit as a decompression-bomb guard.
	raw, err := io.ReadAll(io.LimitReader(src, c.readLimit+1))
	if err != nil {
		return 0, nil, c.abort(err)
	}
	if int64(len(raw)) > c.readLimit {
		return 0, nil, c.failClose(CloseMessageTooBig, errReadLimit)
	}
	data, err := inflate(raw, c.readLimit)
	if err != nil {
		if err == errReadLimit {
			return 0, nil, c.failClose(CloseMessageTooBig, errReadLimit)
		}
		return 0, nil, c.abort(err)
	}
	if c.readMsgType == TextMessage && !utf8.Valid(data) {
		return 0, nil, c.failClose(CloseInvalidFramePayloadData,
			protocolError("invalid UTF-8 in text message"))
	}
	c.inMessage = false
	return c.readMsgType, bytes.NewReader(data), nil
}

// nextDataFrame reads frame headers until a data frame (text, binary or
// continuation) appears, fully handling any control frames along the way.
func (c *Conn) nextDataFrame() (MessageType, bool, error) {
	for {
		opcode, compressed, err := c.readFrameHeader()
		if err != nil {
			return 0, false, err
		}
		if isControl(opcode) {
			if err := c.handleControl(opcode); err != nil {
				return 0, false, err
			}
			continue
		}
		return opcode, compressed, nil
	}
}

// handleControl reads and acts on a control frame. A close frame returns a
// *CloseError; a ping is answered with a pong unless a handler overrides it.
func (c *Conn) handleControl(opcode MessageType) error {
	payload := make([]byte, c.readRemaining)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		if err == io.EOF {
			err = errUnexpectedEOF
		}
		return err
	}
	if c.readMasked {
		maskBytes(c.readMaskKey, 0, payload)
	}
	c.readRemaining = 0

	switch opcode {
	case PingMessage:
		if c.pingHandler != nil {
			return c.pingHandler(string(payload))
		}
		err := c.WriteControl(PongMessage, payload, time.Now().Add(defaultControlDeadline))
		if err == ErrCloseSent {
			return nil
		}
		return err

	case PongMessage:
		if c.pongHandler != nil {
			return c.pongHandler(string(payload))
		}
		return nil

	case CloseMessage:
		code := CloseNoStatusReceived
		text := ""
		echo := []byte{}
		switch {
		case len(payload) == 0:
			// No status code.
		case len(payload) == 1:
			return protocolError("invalid close payload length")
		default:
			code = CloseCode(binary.BigEndian.Uint16(payload))
			if !isValidReceivedCloseCode(code) {
				return protocolError("invalid close code")
			}
			if !utf8.Valid(payload[2:]) {
				return protocolError("invalid UTF-8 in close reason")
			}
			text = string(payload[2:])
			echo = formatCloseMessage(code, "")
		}
		if c.closeHandler != nil {
			_ = c.closeHandler(code, text)
		} else {
			_ = c.WriteControl(CloseMessage, echo,
				time.Now().Add(defaultControlDeadline))
		}
		return &CloseError{Code: code, Text: text}
	}
	return nil
}

// frameSource is an io.Reader over the raw (unmasked) payload bytes of the
// current message, crossing frame boundaries and handling interleaved control
// frames. It returns io.EOF at the end of the message.
type frameSource struct{ c *Conn }

func (s *frameSource) Read(p []byte) (int, error) {
	c := s.c
	for c.readRemaining == 0 {
		if c.readFinal {
			return 0, io.EOF
		}
		opcode, compressed, err := c.nextDataFrame()
		if err != nil {
			return 0, err
		}
		if opcode != continuationFrame {
			return 0, protocolError("expected a continuation frame")
		}
		if compressed {
			return 0, protocolError("continuation frame sets RSV1")
		}
	}
	return c.readPayload(p)
}

// messageReader streams an uncompressed message to the application, enforcing
// the read limit.
type messageReader struct {
	c   *Conn
	src io.Reader
}

func (r *messageReader) Read(p []byte) (int, error) {
	c := r.c
	if c.readErr != nil {
		return 0, c.readErr
	}
	n, err := r.src.Read(p)
	if n > 0 {
		c.readLength += int64(n)
		if c.readLimit > 0 && c.readLength > c.readLimit {
			return n, c.failClose(CloseMessageTooBig, errReadLimit)
		}
	}
	if err == io.EOF {
		c.inMessage = false
	} else if err != nil {
		err = c.abort(err)
	}
	return n, err
}

// readPayload reads up to len(p) bytes of the current frame's payload, unmasking
// in place. It returns io.EOF when the frame is exhausted.
func (c *Conn) readPayload(p []byte) (int, error) {
	if c.readRemaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > c.readRemaining {
		p = p[:c.readRemaining]
	}
	n, err := c.br.Read(p)
	if n > 0 {
		if c.readMasked {
			c.readMaskPos = maskBytes(c.readMaskKey, c.readMaskPos, p[:n])
		}
		c.readRemaining -= int64(n)
	}
	if err == io.EOF {
		err = errUnexpectedEOF
	}
	return n, err
}

// drainMessage discards any unread bytes of the current message.
func (c *Conn) drainMessage() error {
	src := &frameSource{c: c}
	buf := make([]byte, 4096)
	for {
		_, err := src.Read(buf)
		if err == io.EOF {
			c.inMessage = false
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// failClose sends a close frame with the given code and records err as the read
// error. It is used for locally detected conditions (limit exceeded, invalid
// UTF-8) that map to a specific close code.
func (c *Conn) failClose(code CloseCode, err error) error {
	_ = c.WriteControl(CloseMessage, formatCloseMessage(code, ""),
		time.Now().Add(defaultControlDeadline))
	if c.readErr == nil {
		c.readErr = err
	}
	return err
}
