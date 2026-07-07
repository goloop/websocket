package websocket

import (
	"bufio"
	"bytes"
	"compress/flate"
	"net"
	"testing"
	"time"
)

// nopConn is a net.Conn whose writes are discarded and whose reads block never
// happen (the Conn reads from its bufio.Reader instead). It lets a Conn parse a
// fixed byte slice without a real network.
type nopConn struct{}

func (nopConn) Read([]byte) (int, error)         { return 0, nil }
func (nopConn) Write(b []byte) (int, error)      { return len(b), nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return nil }
func (nopConn) RemoteAddr() net.Addr             { return nil }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

func newFuzzConn(data []byte) *Conn {
	return newConn(nopConn{}, false, bufio.NewReader(bytes.NewReader(data)),
		"", true, flate.DefaultCompression)
}

// FuzzFrameParser feeds arbitrary bytes to the reader and asserts it never
// panics, whatever the framing.
func FuzzFrameParser(f *testing.F) {
	f.Add([]byte{0x81, 0x02, 'h', 'i'})
	f.Add([]byte{0x88, 0x00})
	f.Add([]byte{0x89, 0x01, 'p'})
	f.Add(buildFrame(true, false, TextMessage, false, []byte("abc")))
	f.Add(buildFrame(true, true, BinaryMessage, false, []byte("compressed?")))

	f.Fuzz(func(t *testing.T, data []byte) {
		c := newFuzzConn(data)
		for i := 0; i < 64; i++ {
			if _, _, err := c.ReadMessage(); err != nil {
				break
			}
		}
	})
}

// FuzzInflate asserts the decompressor never panics and always respects the
// output limit, guarding against decompression bombs.
func FuzzInflate(f *testing.F) {
	f.Add([]byte{0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		const limit = 1 << 16
		out, err := inflate(data, limit)
		if err == nil && int64(len(out)) > limit {
			t.Fatalf("inflate exceeded the limit: %d", len(out))
		}
	})
}

// FuzzMask asserts masking is always its own inverse from the same position.
func FuzzMask(f *testing.F) {
	f.Add([]byte("payload"), uint32(0x11223344), 0)
	f.Fuzz(func(t *testing.T, data []byte, k uint32, pos int) {
		if pos < 0 {
			pos = -pos
		}
		key := [4]byte{byte(k), byte(k >> 8), byte(k >> 16), byte(k >> 24)}
		b := append([]byte(nil), data...)
		maskBytes(key, pos, b)
		maskBytes(key, pos, b)
		if !bytes.Equal(b, data) {
			t.Fatal("masking twice from the same position did not restore the data")
		}
	})
}
