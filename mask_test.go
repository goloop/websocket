package websocket

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func referenceMask(key [4]byte, b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[i] = b[i] ^ key[i&3]
	}
	return out
}

func TestMaskMatchesReference(t *testing.T) {
	key := [4]byte{0x12, 0x34, 0x56, 0x78}
	for _, size := range []int{0, 1, 3, 4, 7, 8, 9, 15, 16, 17, 100, 1000, 4096} {
		orig := make([]byte, size)
		rand.Read(orig)

		got := append([]byte(nil), orig...)
		maskBytes(key, 0, got)
		if want := referenceMask(key, orig); !bytes.Equal(got, want) {
			t.Fatalf("size %d: mask does not match reference XOR", size)
		}

		// Masking again with the same key restores the original.
		maskBytes(key, 0, got)
		if !bytes.Equal(got, orig) {
			t.Fatalf("size %d: mask is not its own inverse", size)
		}
	}
}

func TestMaskPositionContinuity(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}
	orig := make([]byte, 37)
	rand.Read(orig)

	// Mask in three uneven chunks, threading the returned position through.
	got := append([]byte(nil), orig...)
	pos := maskBytes(key, 0, got[:5])
	pos = maskBytes(key, pos, got[5:20])
	maskBytes(key, pos, got[20:])

	if want := referenceMask(key, orig); !bytes.Equal(got, want) {
		t.Fatal("chunked masking does not match a single-shot mask")
	}
}
