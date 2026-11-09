package cluster

import (
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/session"
)

type fakeAddr struct{}

func (fakeAddr) Network() string { return "tcp" }
func (fakeAddr) String() string  { return "127.0.0.1:0" }

// fakeConn discards all writes and only satisfies net.Conn
type fakeConn struct{}

func (fakeConn) Read([]byte) (int, error)         { return 0, nil }
func (fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (fakeConn) Close() error                     { return nil }
func (fakeConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (fakeConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (fakeConn) SetDeadline(time.Time) error      { return nil }
func (fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (fakeConn) SetWriteDeadline(time.Time) error { return nil }

func TestProcessPacketStateMachine(t *testing.T) {
	newTestAgent := func() (*LocalHandler, *agent) {
		return &LocalHandler{}, newAgent(fakeConn{}, nil, nil)
	}

	t.Run("ack before handshake is rejected", func(t *testing.T) {
		h, a := newTestAgent()
		err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck})
		if err == nil {
			t.Fatal("expect error")
		}
		if a.status() >= statusWorking {
			t.Fatalf("agent must not enter working state, got %d", a.status())
		}
	})

	t.Run("data before handshake is rejected", func(t *testing.T) {
		h, a := newTestAgent()
		err := h.processPacket(a, &packet.Packet{Type: packet.Data, Data: []byte{0x01, 0x00}})
		if err == nil || !strings.Contains(err.Error(), "not yet ACK") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("duplicate handshake is rejected", func(t *testing.T) {
		h, a := newTestAgent()
		if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err != nil {
			t.Fatal(err)
		}
		if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err == nil {
			t.Fatal("expect error on second handshake")
		}
	})

	t.Run("duplicate ack is rejected", func(t *testing.T) {
		h, a := newTestAgent()
		for _, typ := range []packet.Type{packet.Handshake, packet.HandshakeAck} {
			if err := h.processPacket(a, &packet.Packet{Type: typ}); err != nil {
				t.Fatal(err)
			}
		}
		if a.status() != statusWorking {
			t.Fatalf("expect working, got %d", a.status())
		}
		if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err == nil {
			t.Fatal("expect error on second ack")
		}
	})

	t.Run("truncated compressed route returns error without panic", func(t *testing.T) {
		h, a := newTestAgent()
		for _, typ := range []packet.Type{packet.Handshake, packet.HandshakeAck} {
			if err := h.processPacket(a, &packet.Packet{Type: typ}); err != nil {
				t.Fatal(err)
			}
		}
		// Notify with the compressed-route flag but only one byte of route code
		err := h.processPacket(a, &packet.Packet{Type: packet.Data, Data: []byte{0x03, 0x00}})
		if err == nil {
			t.Fatal("expect error")
		}
	})
}

func TestNodeRemoveSessionIdempotent(t *testing.T) {
	n := &Node{}
	n.sessions = map[int64]*session.Session{}
	a := newAgent(fakeConn{}, nil, nil)
	n.storeSession(a.session)

	if _, found := n.removeSession(a.session.ID()); !found {
		t.Fatal("expect session found on first remove")
	}
	if _, found := n.removeSession(a.session.ID()); found {
		t.Fatal("expect session not found on second remove")
	}
	if n.findSession(a.session.ID()) != nil {
		t.Fatal("session must be removed")
	}
}

func TestPacketsBeforeHandshakeDoNotRefreshLastAt(t *testing.T) {
	h := &LocalHandler{}
	a := newAgent(fakeConn{}, nil, nil)
	old := time.Now().Add(-time.Hour).Unix()
	atomic.StoreInt64(&a.lastAt, old)

	for _, typ := range []packet.Type{packet.Heartbeat, packet.Kick} {
		if err := h.processPacket(a, &packet.Packet{Type: typ}); err != nil {
			t.Fatal(err)
		}
		if got := atomic.LoadInt64(&a.lastAt); got != old {
			t.Fatalf("packet type %d must not refresh lastAt before handshake, got %d", typ, got)
		}
	}

	if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt64(&a.lastAt, old)
	if err := h.processPacket(a, &packet.Packet{Type: packet.Heartbeat}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&a.lastAt); got == old {
		t.Fatal("lastAt must be refreshed after handshake")
	}
}

// scriptConn feeds scripted data to the read loop; Read returns EOF after Close
type scriptConn struct {
	fakeConn
	reads  chan []byte
	closed chan struct{}
	once   sync.Once
}

func newScriptConn() *scriptConn {
	return &scriptConn{reads: make(chan []byte, 16), closed: make(chan struct{})}
}

func (c *scriptConn) Read(b []byte) (int, error) {
	select {
	case data := <-c.reads:
		return copy(b, data), nil
	case <-c.closed:
		return 0, io.EOF
	}
}

func (c *scriptConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *scriptConn) feed(t *testing.T, typ packet.Type, data []byte) {
	t.Helper()
	p, err := codec.Encode(typ, data)
	if err != nil {
		t.Fatal(err)
	}
	c.reads <- p
}

const testRoute = "Test.Handler"

func newTestNode(pip pipeline.Pipeline) (*Node, *LocalHandler) {
	n := &Node{sessions: map[int64]*session.Session{}}
	n.cluster = newCluster(n)
	n.handler = NewHandler(n, pip)
	n.handler.localHandlers[testRoute] = &component.Handler{IsRawArg: true}
	return n, n.handler
}

func sessionCount(n *Node) int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.sessions)
}

func firstSession(n *Node) *session.Session {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, s := range n.sessions {
		return s
	}
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func runHandle(h *LocalHandler, conn net.Conn) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.handle(conn)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handle did not exit")
	}
}

func TestHandleRemovesSessionOnExit(t *testing.T) {
	n, h := newTestNode(nil)
	conn := newScriptConn()
	done := runHandle(h, conn)

	waitFor(t, "session stored", func() bool { return sessionCount(n) == 1 })
	conn.Close()
	waitDone(t, done)

	if got := sessionCount(n); got != 0 {
		t.Fatalf("session leaked after connection closed, count=%d", got)
	}
}

func TestHandleRecoversInboundPanic(t *testing.T) {
	pip := pipeline.New()
	pip.Inbound().PushBack(func(*session.Session, *pipeline.Message) error { panic("inbound boom") })
	n, h := newTestNode(pip)
	conn := newScriptConn()
	done := runHandle(h, conn)

	waitFor(t, "session stored", func() bool { return sessionCount(n) == 1 })
	s := firstSession(n)

	conn.feed(t, packet.Handshake, nil)
	conn.feed(t, packet.HandshakeAck, nil)
	m := &message.Message{Type: message.Notify, Route: testRoute, Data: []byte("x")}
	em, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	conn.feed(t, packet.Data, em)

	waitDone(t, done)
	if got := s.CloseReason(); got != "panic" {
		t.Fatalf("close reason = %q, want panic", got)
	}
	if got := sessionCount(n); got != 0 {
		t.Fatalf("session leaked after panic, count=%d", got)
	}
}

func TestWriteRecoversOutboundPanic(t *testing.T) {
	pip := pipeline.New()
	pip.Outbound().PushBack(func(*session.Session, *pipeline.Message) error { panic("outbound boom") })
	n, h := newTestNode(pip)
	conn := newScriptConn()
	done := runHandle(h, conn)

	waitFor(t, "session stored", func() bool { return sessionCount(n) == 1 })
	s := firstSession(n)

	if err := s.Push("test.push", []byte("x")); err != nil {
		t.Fatal(err)
	}

	waitDone(t, done)
	if got := s.CloseReason(); got != "panic" {
		t.Fatalf("close reason = %q, want panic", got)
	}
	if got := sessionCount(n); got != 0 {
		t.Fatalf("session leaked after panic, count=%d", got)
	}
}
