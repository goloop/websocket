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
		return c.readMsgType, &messageReader{
			c: c, src: src, text: opcode == TextMessage,
		}, nil
	}

	// Compressed: read the whole (bounded) compressed message, then inflate with
	// the read limit as a decompression-bomb guard. The read limit applies to
	// the decompressed size, so the compressed cap allows for deflate's small
	// worst-case expansion on incompressible data; the exact limit is enforced
	// by inflate on the output.
	compressedCap := c.readLimit + c.readLimit/8 + 64
	raw, err := io.ReadAll(io.LimitReader(src, compressedCap+1))
	if err != nil {
		return 0, nil, c.abort(err)
	}
	if int64(len(raw)) > compressedCap {
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

// Read pulls the next chunk of the current message's payload, advancing across
// data frames as needed. It requires each frame after the first to be an
// uncompressed continuation frame and returns io.EOF once the final frame is
// exhausted.
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
// the read limit and, for text messages, validating UTF-8 incrementally so a
// rune split across a frame or Read boundary is handled correctly.
type messageReader struct {
	c     *Conn
	src   io.Reader
	text  bool
	valid utf8Validator
}

// Read streams the message payload to the caller, propagating any sticky read
// error and, for text messages, validating UTF-8 incrementally so a rune split
// across frame or Read boundaries is still checked correctly.
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
		if r.text {
			if verr := r.valid.write(p[:n]); verr != nil {
				return n, c.failClose(CloseInvalidFramePayloadData, verr)
			}
		}
	}
	if err == io.EOF {
		if r.text {
			if verr := r.valid.done(); verr != nil {
				return n, c.failClose(CloseInvalidFramePayloadData, verr)
			}
		}
		c.inMessage = false
	} else if err != nil {
		err = c.abort(err)
	}
	return n, err
}

// utf8Validator incrementally checks a byte stream for valid UTF-8, tolerating
// a rune whose bytes are split across successive write calls.
type utf8Validator struct {
	pending []byte // bytes of an incomplete trailing rune (1..3 bytes)
}

// write validates the next chunk of bytes, carrying over the bytes of an
// incomplete trailing rune. It returns errInvalidUTF8 on an invalid sequence.
func (v *utf8Validator) write(p []byte) error {
	if len(v.pending) > 0 {
		p = append(v.pending, p...)
		v.pending = nil
	}
	for len(p) > 0 {
		if p[0] < utf8.RuneSelf {
			p = p[1:]
			continue
		}
		r, size := utf8.DecodeRune(p)
		if r == utf8.RuneError && size == 1 {
			if !utf8.FullRune(p) {
				// A valid prefix of a rune split across the boundary: carry it.
				v.pending = append(v.pending[:0], p...)
				return nil
			}
			return errInvalidUTF8
		}
		p = p[size:]
	}
	return nil
}

// done reports whether the stream ended on a rune boundary.
func (v *utf8Validator) done() error {
	if len(v.pending) > 0 {
		return errInvalidUTF8
	}
	return nil
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

// drainMessage discards any unread bytes of the current message. The read limit
// still applies while draining, so a peer cannot force an unbounded discard by
// leaving a huge message unread.
func (c *Conn) drainMessage() error {
	src := &frameSource{c: c}
	buf := make([]byte, 4096)
	var drained int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			drained += int64(n)
			if c.readLimit > 0 && drained > c.readLimit {
				return c.failClose(CloseMessageTooBig, errReadLimit)
			}
		}
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
