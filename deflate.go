package websocket

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
)

// deflateSyncTail is the four octets that a DEFLATE sync flush ends with. Under
// permessage-deflate (RFC 7692) the sender strips them from an outgoing message.
var deflateSyncTail = []byte{0x00, 0x00, 0xff, 0xff}

// inflateTail is appended before decompressing: the sync-flush octets the sender
// removed, followed by a final empty stored block (0x01 0x00 0x00 0xff 0xff).
// The final block lets flate terminate the headerless stream cleanly instead of
// reporting an unexpected EOF after the data.
var inflateTail = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

// flate compression levels span flate.HuffmanOnly (-2) to flate.BestCompression
// (9); shift by two to index a pool per level.
const flateLevelOffset = -flate.HuffmanOnly // 2

var flateWriterPools [flate.BestCompression + flateLevelOffset + 1]sync.Pool

var flateReaderPool = sync.Pool{
	New: func() any { return flate.NewReader(nil) },
}

// deflate compresses data for a single message with "no context takeover": each
// message is independent. It returns the compressed payload with the trailing
// sync-flush octets removed, as required by RFC 7692.
func deflate(data []byte, level int) ([]byte, error) {
	if !isValidCompressionLevel(level) {
		level = flate.DefaultCompression
	}
	var buf bytes.Buffer
	fw := getFlateWriter(&buf, level)
	_, werr := fw.Write(data)
	ferr := fw.Flush() // a sync flush terminates the block with deflateTail
	putFlateWriter(fw, level)
	if werr != nil {
		return nil, werr
	}
	if ferr != nil {
		return nil, ferr
	}

	out := buf.Bytes()
	if n := len(out); n >= 4 && bytes.Equal(out[n-4:], deflateSyncTail) {
		out = out[:n-4]
	}
	return out, nil
}

// inflate decompresses a permessage-deflate payload, appending the sync-flush
// tail first. It stops once the output would exceed limit, returning
// errReadLimit, which guards against decompression bombs.
func inflate(payload []byte, limit int64) ([]byte, error) {
	fr := flateReaderPool.Get().(io.ReadCloser)
	defer flateReaderPool.Put(fr)

	src := io.MultiReader(bytes.NewReader(payload), bytes.NewReader(inflateTail))
	if err := fr.(flate.Resetter).Reset(src, nil); err != nil {
		return nil, err
	}

	out, err := io.ReadAll(io.LimitReader(fr, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, errReadLimit
	}
	return out, nil
}

func isValidCompressionLevel(level int) bool {
	return flate.HuffmanOnly <= level && level <= flate.BestCompression
}

func getFlateWriter(w io.Writer, level int) *flate.Writer {
	p := &flateWriterPools[level+flateLevelOffset]
	if v := p.Get(); v != nil {
		fw := v.(*flate.Writer)
		fw.Reset(w)
		return fw
	}
	fw, _ := flate.NewWriter(w, level)
	return fw
}

func putFlateWriter(fw *flate.Writer, level int) {
	flateWriterPools[level+flateLevelOffset].Put(fw)
}
