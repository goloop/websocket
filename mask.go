package websocket

import (
	"crypto/rand"
	"encoding/binary"
)

// newMaskKey returns a fresh 4-byte masking key. Clients must mask every frame
// they send (RFC 6455 section 5.3); the key must be unpredictable, so it comes
// from crypto/rand.
func newMaskKey() [4]byte {
	var key [4]byte
	//nolint:errcheck // crypto/rand.Read does not fail on supported platforms
	rand.Read(key[:])
	return key
}

// maskBytes applies the masking transform to b in place, starting at key
// position pos, and returns the next key position. It XORs eight bytes at a
// time for speed, using only encoding/binary (no package unsafe). The pos
// argument lets a frame payload be masked across several calls.
func maskBytes(key [4]byte, pos int, b []byte) int {
	// Rotate the key so that rot[0] lines up with the current position; then
	// byte i of b is XORed with rot[i mod 4].
	rot := key
	if pos &= 3; pos != 0 {
		for i := 0; i < 4; i++ {
			rot[i] = key[(pos+i)&3]
		}
	}

	// Build an eight-byte word holding the rotated key twice, so a 64-bit XOR
	// masks eight aligned bytes at once.
	var kw [8]byte
	copy(kw[0:4], rot[:])
	copy(kw[4:8], rot[:])
	word := binary.LittleEndian.Uint64(kw[:])

	i, n := 0, len(b)
	for ; i+8 <= n; i += 8 {
		v := binary.LittleEndian.Uint64(b[i:])
		binary.LittleEndian.PutUint64(b[i:], v^word)
	}
	for ; i < n; i++ {
		b[i] ^= rot[i&3]
	}

	return (pos + n) & 3
}
