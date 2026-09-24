package websocket

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"strconv"
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
// offer includes a permessage-deflate configuration this server can honour.
//
// The server always replies with the full window and no context takeover, so
// it accepts the no-context-takeover flags and client_max_window_bits, and
// declines anything else. Declining means compression is simply not enabled,
// which is allowed, rather than answering an offer incorrectly.
//
// client_max_window_bits is accepted and not echoed. Without a value it only
// tells the server it may cap the client's window, an offer this server does
// not take up; with a value it promises the client will compress with a
// window no larger than that, which this server's full-size window can always
// decompress. Declining the whole offer over it meant no compression at all
// with the browsers that send it.
func serverAcceptsDeflateOffer(header http.Header) bool {
	for _, line := range header.Values("Sec-WebSocket-Extensions") {
		for _, ext := range strings.Split(line, ",") {
			parts := strings.Split(ext, ";")
			if !strings.EqualFold(strings.TrimSpace(parts[0]), permessageDeflate) {
				continue
			}
			if deflateOfferAcceptable(parts[1:]) {
				return true
			}
		}
	}
	return false
}

// deflateOfferAcceptable reports whether every parameter of one offer is one
// this server can live with. A repeated parameter is refused: an offer that
// says a thing twice has not said it more clearly.
func deflateOfferAcceptable(params []string) bool {
	seen := map[string]bool{}
	for _, p := range params {
		name, value, hasValue := strings.Cut(strings.TrimSpace(p), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.Trim(strings.TrimSpace(value), `"`)
		if name == "" || seen[name] {
			return false
		}
		seen[name] = true

		switch name {
		case "client_no_context_takeover", "server_no_context_takeover":
			if hasValue && value != "" {
				return false // these flags take no value
			}
		case "client_max_window_bits":
			if hasValue && !validWindowBits(value) {
				return false
			}
		default:
			// server_max_window_bits would require this server to shrink its
			// own window, which it cannot do, and anything else is unknown.
			return false
		}
	}
	return true
}

// validWindowBits reports whether s is a window size this protocol allows.
func validWindowBits(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n >= 8 && n <= 15
}

// validateServerExtensions checks the whole Sec-WebSocket-Extensions reply,
// not just the part this client hoped for. A server may only select an
// extension the client offered, so anything else in that header means the two
// ends disagree about what the connection is, and the handshake is refused.
//
// It reports whether permessage-deflate was negotiated.
func validateServerExtensions(header http.Header, offeredDeflate bool) (bool, error) {
	var deflates []string
	for _, line := range header.Values("Sec-WebSocket-Extensions") {
		for _, ext := range strings.Split(line, ",") {
			if strings.TrimSpace(ext) == "" {
				if len(deflates) == 0 && len(header.Values("Sec-WebSocket-Extensions")) == 1 &&
					strings.TrimSpace(line) == "" {
					continue // an empty header selects nothing
				}
				return false, ErrBadHandshake
			}
			name := strings.TrimSpace(strings.Split(ext, ";")[0])
			if !strings.EqualFold(name, permessageDeflate) {
				return false, ErrBadHandshake // never offered
			}
			deflates = append(deflates, ext)
		}
	}

	switch {
	case len(deflates) == 0:
		return false, nil
	case !offeredDeflate, len(deflates) > 1:
		return false, ErrBadHandshake
	}
	return deflateParamsAcceptable(deflates[0])
}

// deflateParamsAcceptable checks one permessage-deflate offer's parameters.
// This client always resets its context per message and uses the default
// window, so the server has to confirm server_no_context_takeover and ask for
// nothing else. Neither flag takes a value, and neither may be repeated.
func deflateParamsAcceptable(ext string) (bool, error) {
	seen := map[string]bool{}
	serverNoCtx := false
	for _, p := range strings.Split(ext, ";")[1:] {
		name, value, hasValue := strings.Cut(strings.TrimSpace(p), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		switch name {
		case "client_no_context_takeover", "server_no_context_takeover":
			if hasValue && strings.TrimSpace(value) != "" {
				return false, ErrBadHandshake // these flags take no value
			}
			if seen[name] {
				return false, ErrBadHandshake // repeated parameter
			}
			seen[name] = true
			serverNoCtx = serverNoCtx || name == "server_no_context_takeover"
		default:
			return false, ErrBadHandshake // window bits or something unknown
		}
	}
	if !serverNoCtx {
		// The inflate context is reset per message, so the server must also
		// disable context takeover or later messages would not decompress.
		return false, ErrBadHandshake
	}
	return true, nil
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
