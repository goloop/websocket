package websocket

import (
	"bytes"
	"testing"
)

func BenchmarkSplitRuneValidation(b *testing.B) {
	first := []byte{0xe2}
	rest := append([]byte{0x82, 0xac}, bytes.Repeat([]byte("x"), 65536)...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v utf8Validator
		if err := v.write(first); err != nil {
			b.Fatal(err)
		}
		if err := v.write(rest); err != nil {
			b.Fatal(err)
		}
		if err := v.done(); err != nil {
			b.Fatal(err)
		}
	}
}
