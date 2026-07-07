package websocket

import (
	"bytes"
	"compress/flate"
	"testing"
)

func BenchmarkMask4K(b *testing.B) {
	key := [4]byte{0x12, 0x34, 0x56, 0x78}
	buf := make([]byte, 4096)
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		maskBytes(key, 0, buf)
	}
}

func BenchmarkWriteMessage(b *testing.B) {
	c := newConn(nopConn{}, true, nil, "", false, flate.DefaultCompression)
	payload := bytes.Repeat([]byte("x"), 512)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.WriteMessage(BinaryMessage, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeflateRoundtrip(b *testing.B) {
	payload := bytes.Repeat([]byte("the quick brown fox "), 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := deflate(payload, flate.DefaultCompression)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := inflate(out, 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
