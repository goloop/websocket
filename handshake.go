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

// serverAcceptsDeflateOffer reports whether a client's Sec-WebSocket-Extensions
// offer includes a permessage-deflate configuration this server can honour. The
// server always replies with the full window and no context takeover, so it
// only accepts an offer whose parameters are a subset of the no-context-takeover
// flags; a window-bits constraint or an unknown parameter means the offer is
// declined (compression is simply not enabled) rather than answered incorrectly.
func serverAcceptsDeflateOffer(header http.Header) bool {
	for _, line := range header.Values("Sec-WebSocket-Extensions") {
		for _, ext := range strings.Split(line, ",") {
			parts := strings.Split(ext, ";")
			if !strings.EqualFold(strings.TrimSpace(parts[0]), permessageDeflate) {
				continue
			}
			ok := true
			for _, p := range parts[1:] {
				name, _, _ := strings.Cut(strings.TrimSpace(p), "=")
				switch strings.ToLower(strings.TrimSpace(name)) {
				case "client_no_context_takeover", "server_no_context_takeover":
					// Compatible with our fixed no-context-takeover behaviour.
				default:
					ok = false
				}
			}
			if ok {
				return true
			}
		}
	}
	return false
}

// clientAcceptsDeflateResponse validates the server's permessage-deflate
// response. It reports whether compression is enabled and errors when the
// server selected the extension with parameters this client cannot honour: it
// only supports no-context-takeover with the default window, so the server must
// confirm server_no_context_takeover and use no other parameters.
func clientAcceptsDeflateResponse(header http.Header) (bool, error) {
	for _, line := range header.Values("Sec-WebSocket-Extensions") {
		for _, ext := range strings.Split(line, ",") {
			parts := strings.Split(ext, ";")
			if !strings.EqualFold(strings.TrimSpace(parts[0]), permessageDeflate) {
				continue
			}
			serverNoCtx := false
			for _, p := range parts[1:] {
				name, _, _ := strings.Cut(strings.TrimSpace(p), "=")
				switch strings.ToLower(strings.TrimSpace(name)) {
				case "server_no_context_takeover":
					serverNoCtx = true
				case "client_no_context_takeover":
					// Acceptable: this client always resets its own context.
				default:
					// An unknown or unsupported parameter (window bits, ...).
					return false, ErrBadHandshake
				}
			}
			if !serverNoCtx {
				// The inflate context is reset per message, so the server must
				// also disable context takeover or later messages would not
				// decompress correctly.
				return false, ErrBadHandshake
			}
			return true, nil
		}
	}
	return false, nil
}

// IsWebSocketUpgrade reports whether r is a WebSocket upgrade request.
func IsWebSocketUpgrade(r *http.Request) bool {
	return tokenListContainsValue(r.Header, "Connection", "upgrade") &&
		tokenListContainsValue(r.Header, "Upgrade", "websocket")
}

// Subprotocols returns the subprotocols listed in the request's
// Sec-WebSocket-Protocol header. The header may be split across several field
// lines, all of which are considered.
func Subprotocols(r *http.Request) []string {
	var protocols []string
	for _, line := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, tok := range strings.Split(line, ",") {
			if tok = strings.TrimSpace(tok); tok != "" {
				protocols = append(protocols, tok)
			}
		}
	}
	return protocols
}
