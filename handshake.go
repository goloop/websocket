package websocket

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"strings"
)

// keyGUID is the fixed value RFC 6455 (section 1.3) concatenates with the client
// key to derive Sec-WebSocket-Accept. SHA-1 is mandated here only for this
// handshake; it is not used as a security primitive.
const keyGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// permessageDeflate is the extension token for RFC 7692 compression.
const permessageDeflate = "permessage-deflate"

// computeAcceptKey returns the Sec-WebSocket-Accept value for a client key.
func computeAcceptKey(challengeKey string) string {
	h := sha1.New() //nolint:gosec // SHA-1 is required by RFC 6455 for the handshake
	h.Write([]byte(challengeKey))
	h.Write([]byte(keyGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// generateChallengeKey returns a fresh, random Sec-WebSocket-Key for a client.
func generateChallengeKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// tokenListContainsValue reports whether a comma-separated header field contains
// a token equal (case-insensitively) to value.
func tokenListContainsValue(header http.Header, name, value string) bool {
	for _, line := range header.Values(name) {
		for _, tok := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), value) {
				return true
			}
		}
	}
	return false
}

// headerOffersDeflate reports whether a Sec-WebSocket-Extensions field offers
// permessage-deflate.
func headerOffersDeflate(header http.Header) bool {
	for _, line := range header.Values("Sec-WebSocket-Extensions") {
		for _, ext := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(strings.Split(ext, ";")[0]), permessageDeflate) {
				return true
			}
		}
	}
	return false
}

// IsWebSocketUpgrade reports whether r is a WebSocket upgrade request.
func IsWebSocketUpgrade(r *http.Request) bool {
	return tokenListContainsValue(r.Header, "Connection", "upgrade") &&
		tokenListContainsValue(r.Header, "Upgrade", "websocket")
}

// Subprotocols returns the subprotocols listed in the request's
// Sec-WebSocket-Protocol header.
func Subprotocols(r *http.Request) []string {
	h := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Protocol"))
	if h == "" {
		return nil
	}
	protocols := strings.Split(h, ",")
	for i := range protocols {
		protocols[i] = strings.TrimSpace(protocols[i])
	}
	return protocols
}
