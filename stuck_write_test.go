package websocket

import (
	"errors"
	"net"
	"testing"
	"time"
)

// stuckConn returns a server-side Conn whose peer never reads, so any data
// write blocks for good, plus the peer end to close when the test is done.
func stuckConn(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	srv, peer := net.Pipe()
	c := newConn(srv, true, nil, "", false, 0)
	t.Cleanup(func() { srv.Close(); peer.Close() })

	started := make(chan struct{})
	go func() {
		close(started)
		_ = c.WriteMessage(TextMessage, []byte("nobody reads this"))
	}()
	<-started
	// Wait until the write actually holds the lock and is stuck in the pipe.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(c.writeLock) == 1 {
			return c, peer
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the data write never took the write lock")
	return nil, nil
}

// A control write must honour its deadline even while a data write to a peer
// that stopped reading holds the write lock, or the reader's close and pong
// frames hang with it.
func TestWriteControlGivesUpBehindStuckWrite(t *testing.T) {
	c, _ := stuckConn(t)

	start := time.Now()
	err := c.WriteControl(PingMessage, nil, time.Now().Add(30*time.Millisecond))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("WriteControl returned nil while the connection is stuck")
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Errorf("WriteControl error = %v, want a timeout", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("WriteControl took %v, want about its 30ms deadline", elapsed)
	}
}

// Setting a write deadline must not wait behind a stuck write: it is the way
// to end one, exactly as on a net.Conn.
func TestSetWriteDeadlineInterruptsStuckWrite(t *testing.T) {
	c, _ := stuckConn(t)

	done := make(chan error, 1)
	go func() { done <- c.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetWriteDeadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SetWriteDeadline blocked behind the stuck write")
	}

	// The data write is now bounded and must give the lock back.
	select {
	case c.writeLock <- struct{}{}:
		<-c.writeLock
	case <-time.After(time.Second):
		t.Fatal("the stuck write did not end after its deadline passed")
	}
	if c.writeErr == nil {
		t.Error("the timed-out write did not record a write error")
	}
}
