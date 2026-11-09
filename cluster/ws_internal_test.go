package cluster

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type wsResult struct {
	conn *wsConn
	err  error
	cost time.Duration
}

// serveWSConn starts a server that wraps each upgraded connection with newWSConn and reports the outcome.
func serveWSConn(t *testing.T) (*websocket.Conn, <-chan wsResult) {
	t.Helper()
	results := make(chan wsResult, 1)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		start := time.Now()
		c, err := newWSConn(conn, nil)
		results <- wsResult{conn: c, err: err, cost: time.Since(start)}
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, results
}

func setFirstFrameTimeout(t *testing.T, d time.Duration) {
	old := firstFrameTimeout
	firstFrameTimeout = d
	t.Cleanup(func() { firstFrameTimeout = old })
}

func TestNewWSConnFirstFrameTimeout(t *testing.T) {
	setFirstFrameTimeout(t, 100*time.Millisecond)
	_, results := serveWSConn(t)

	select {
	case res := <-results:
		if res.err == nil {
			t.Fatal("expect timeout error when no first frame is sent")
		}
		if res.cost > 2*time.Second {
			t.Fatalf("timeout took too long: %v", res.cost)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("newWSConn did not return")
	}
}

func TestNewWSConnClearsDeadlineAfterFirstFrame(t *testing.T) {
	setFirstFrameTimeout(t, 100*time.Millisecond)
	client, results := serveWSConn(t)

	if err := client.WriteMessage(websocket.BinaryMessage, []byte("first")); err != nil {
		t.Fatal(err)
	}
	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}

	buf := make([]byte, 16)
	n, err := res.conn.Read(buf)
	if err != nil || string(buf[:n]) != "first" {
		t.Fatalf("read first frame: n=%d err=%v", n, err)
	}

	// the second frame arrives after the first-frame timeout has long passed
	go func() {
		time.Sleep(300 * time.Millisecond)
		client.WriteMessage(websocket.BinaryMessage, []byte("second"))
	}()
	// wsConn.Read returns (0, nil) when it crosses a frame boundary, so read until data shows up
	n = 0
	for n == 0 {
		n, err = res.conn.Read(buf)
		if err != nil {
			t.Fatalf("read after timeout window: %v", err)
		}
	}
	if string(buf[:n]) != "second" {
		t.Fatalf("unexpected data: %q", buf[:n])
	}
}

// A silent connection must actually be closed after the first-frame timeout, not just abandoned.
func TestHandleWSClosesConnOnFirstFrameTimeout(t *testing.T) {
	setFirstFrameTimeout(t, 100*time.Millisecond)

	h := &LocalHandler{}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		h.handleWS(conn, nil)
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// the server side is silent too, so the only way this read returns before our deadline is the server closing the socket
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = client.ReadMessage()
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("server did not close the silent connection, err=%v", err)
	}
}
