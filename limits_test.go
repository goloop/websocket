package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"reflect"
	"testing"
)

// The read limit is a budget for the whole message, and skipping the rest of
// one does not hand out a fresh budget: reading half a message and then
// discarding it must not let roughly twice the limit through.
func TestDiscardSharesTheMessageBudget(t *testing.T) {
	wire := append(
		buildFrame(true, false, BinaryMessage, false, bytes.Repeat([]byte("x"), 32)),
		buildFrame(true, false, BinaryMessage, false, []byte("next"))...,
	)
	c := newFuzzConn(wire)
	c.SetReadLimit(16)

	_, r, err := c.NextReader()
	if err != nil {
		if !errors.Is(err, errReadLimit) {
			t.Fatalf("NextReader: %v", err)
		}
		return // rejecting the oversized frame up front is fine too
	}
	if _, err := io.ReadFull(r, make([]byte, 16)); err != nil {
		if !errors.Is(err, errReadLimit) {
			t.Fatalf("read within the limit: %v", err)
		}
		return
	}
	if _, _, err := c.NextReader(); !errors.Is(err, errReadLimit) {
		t.Fatalf("NextReader after a partial read = %v, want errReadLimit", err)
	}
}

// A text message must be checked as UTF-8 even in the part the application
// skipped: the check belongs to the message, not to what was read of it.
func TestDiscardStillChecksUTF8(t *testing.T) {
	// A second, well-formed message follows, so that reaching it without an
	// error is a real result and not just the end of the data.
	wire := append(
		buildFrame(true, false, TextMessage, false, []byte{0x61, 0xff}),
		buildFrame(true, false, TextMessage, false, []byte("ok"))...,
	)
	c := newFuzzConn(wire)

	_, r, err := c.NextReader()
	if err != nil {
		t.Fatalf("NextReader: %v", err)
	}
	if _, err := r.Read(make([]byte, 1)); err != nil {
		t.Fatalf("read the first byte: %v", err)
	}
	if _, _, err := c.NextReader(); err == nil {
		t.Error("invalid UTF-8 in the skipped part of a text message went unreported")
	}
}

// The read limit bounds what reaches the application, not just what is
// counted: an oversized message must not deliver its bytes alongside the
// error, because an io.Reader consumer is entitled to use them.
func TestReadLimitWithholdsOversizedBytes(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, BinaryMessage, false,
		bytes.Repeat([]byte("x"), 64)))
	c.SetReadLimit(16)

	_, r, err := c.NextReader()
	if err != nil {
		if !errors.Is(err, errReadLimit) {
			t.Fatalf("NextReader: %v", err)
		}
		return
	}

	var got int
	buf := make([]byte, 128)
	for {
		n, err := r.Read(buf)
		got += n
		if err != nil {
			if !errors.Is(err, errReadLimit) {
				t.Fatalf("Read: %v", err)
			}
			break
		}
	}
	if int64(got) > 16 {
		t.Errorf("%d bytes reached the caller, want at most the 16-byte limit", got)
	}
}

// A message exactly the size of the limit is allowed: the budget is a limit,
// not a limit minus one.
func TestMessageExactlyAtTheLimitIsAllowed(t *testing.T) {
	c := newFuzzConn(buildFrame(true, false, BinaryMessage, false,
		bytes.Repeat([]byte("x"), 16)))
	c.SetReadLimit(16)

	_, body, err := c.ReadMessage()
	if err != nil || len(body) != 16 {
		t.Fatalf("len=%d err=%v, want a 16-byte message", len(body), err)
	}
}

// SetWriteLimit bounds a message built with NextWriter, and refuses it before
// the buffer grows rather than after.
func TestWriteLimitBoundsTheBuffer(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, flate.DefaultCompression)
	c.SetWriteLimit(32)

	w, err := c.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), 16)); err != nil {
		t.Fatalf("write within the limit: %v", err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), 64)); !errors.Is(err, ErrWriteLimit) {
		t.Fatalf("err = %v, want ErrWriteLimit", err)
	}

	mw := w.(*messageWriter)
	if mw.buf.Len() != 16 {
		t.Errorf("buffer grew to %d bytes past the limit", mw.buf.Len())
	}

	if err := c.WriteMessage(BinaryMessage, bytes.Repeat([]byte("a"), 64)); !errors.Is(err, ErrWriteLimit) {
		t.Errorf("WriteMessage err = %v, want ErrWriteLimit", err)
	}
	// Without a limit the same writes go through, so the default is unchanged.
	c.SetWriteLimit(0)
	if err := c.WriteMessage(BinaryMessage, bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Errorf("unbounded write: %v", err)
	}
}

// A closed writer must not keep the message it sent reachable.
func TestClosedWriterReleasesItsBuffer(t *testing.T) {
	c := newConn(nopConn{}, false, nil, "", false, flate.DefaultCompression)
	w, err := c.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("NextWriter: %v", err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), 1<<20)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mw := w.(*messageWriter)
	if mw.buf.Cap() != 0 {
		t.Errorf("a closed writer still holds %d bytes of buffer", mw.buf.Cap())
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// A pooled compressor must not keep the message it just worked on reachable.
func TestPooledCompressorsReleaseTheirBuffers(t *testing.T) {
	const level = flate.BestSpeed
	big := bytes.Repeat([]byte("payload "), 1<<19) // 4 MiB

	// The writer is checked exactly: the buffer it was given must no longer
	// be reachable from the writer once it is back in the pool.
	var out bytes.Buffer
	fw := getFlateWriter(&out, level)
	if _, err := fw.Write(big); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	putFlateWriter(fw, level)

	if pooled := flateWriterPools[level+flateLevelOffset].Get(); pooled != nil {
		if reaches(reflect.ValueOf(pooled), reflect.ValueOf(&out).Pointer()) {
			t.Error("a pooled flate writer still points at the buffer it wrote to")
		}
	}

	// The reader is checked by size: its own window is tens of kilobytes, so
	// anything near the message size means it is still holding the message.
	broken := append([]byte{0xff, 0xff, 0xff}, big...)
	if _, err := inflate(broken, int64(len(big))); err == nil {
		t.Skip("the broken payload inflated; nothing to check")
	}
	fr := flateReaderPool.Get().(io.ReadCloser)
	defer flateReaderPool.Put(fr)
	if held := retainedBytes(reflect.ValueOf(fr)); held > 256*1024 {
		t.Errorf("a pooled flate reader holds %d bytes of the failed message", held)
	}
}

// reaches reports whether target is reachable from v, which is how a pooled
// object is shown to still hold on to something it should have let go of.
func reaches(v reflect.Value, target uintptr) bool {
	seen := map[uintptr]bool{}
	var walk func(reflect.Value, int) bool
	walk = func(v reflect.Value, depth int) bool {
		if depth > 8 || !v.IsValid() {
			return false
		}
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return false
			}
			if v.Kind() == reflect.Pointer {
				if v.Pointer() == target {
					return true
				}
				if seen[v.Pointer()] {
					return false
				}
				seen[v.Pointer()] = true
			}
			return walk(v.Elem(), depth+1)
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				f := v.Field(i)
				if !f.CanInterface() {
					f = reflect.NewAt(f.Type(), f.Addr().UnsafePointer()).Elem()
				}
				if walk(f, depth+1) {
					return true
				}
			}
		}
		return false
	}
	return walk(v, 0)
}

// retainedBytes walks a value and reports the largest byte slice or buffer it
// can still reach, which is what a pooled object keeps alive.
func retainedBytes(v reflect.Value) int {
	seen := map[uintptr]bool{}
	var walk func(reflect.Value, int) int
	walk = func(v reflect.Value, depth int) int {
		if depth > 6 || !v.IsValid() {
			return 0
		}
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return 0
			}
			if v.Kind() == reflect.Pointer {
				if seen[v.Pointer()] {
					return 0
				}
				seen[v.Pointer()] = true
			}
			return walk(v.Elem(), depth+1)
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return v.Cap()
			}
			return 0
		case reflect.Struct:
			most := 0
			for i := 0; i < v.NumField(); i++ {
				f := v.Field(i)
				if !f.CanInterface() {
					f = reflect.NewAt(f.Type(), f.Addr().UnsafePointer()).Elem()
				}
				if n := walk(f, depth+1); n > most {
					most = n
				}
			}
			return most
		}
		return 0
	}
	return walk(v, 0)
}
