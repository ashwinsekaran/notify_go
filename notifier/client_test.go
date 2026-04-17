package notifier

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// startServer starts a test HTTP server that collects received messages.
// returns the server and a function to get collected messages.
func startServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var received []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(srv.Close)

	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]string, len(received))
		copy(cp, received)
		return cp
	}
}

// waitFor polls condition until it returns true or timeout is reached.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// dlqLines reads all lines from the DLQ file.
func dlqLines(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestNotify_MessageDelivered verifies a sent message is received by the server.
func TestNotify_MessageDelivered(t *testing.T) {
	srv, messages := startServer(t)

	client, _ := New(srv.URL)
	defer client.Close()

	client.Notify("hello")

	ok := waitFor(t, time.Second, func() bool {
		return len(messages()) == 1
	})
	if !ok {
		t.Fatal("message was not delivered within timeout")
	}

	if got := messages()[0]; got != "hello" {
		t.Errorf("expected %q, got %q", "hello", got)
	}
}

// TestNotify_MultipleMessages verifies all messages are delivered.
func TestNotify_MultipleMessages(t *testing.T) {
	srv, messages := startServer(t)

	client, _ := New(srv.URL)
	defer client.Close()

	want := []string{"one", "two", "three"}
	for _, msg := range want {
		client.Notify(msg)
	}

	ok := waitFor(t, time.Second, func() bool {
		return len(messages()) == len(want)
	})
	if !ok {
		t.Fatalf("expected %d messages, got %d", len(want), len(messages()))
	}
}

// TestNotify_QueueFull verifies messages are parked in DLQ when queue is full.
func TestNotify_QueueFull(t *testing.T) {
	testDLQPath := "dlq/test_failed.log"
	os.Remove(testDLQPath)
	t.Cleanup(func() { os.Remove(testDLQPath) })

	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked
	}))

	client, _ := New(srv.URL, Config{Workers: 1, QueueSize: 1})
	client.dlq.path = testDLQPath // use test-specific DLQ file

	// worker picks up msg1 and blocks on server
	// msg2 fills the queue (size=1)
	// msg3 — queue full → should be parked in DLQ
	client.Notify("msg1")
	time.Sleep(50 * time.Millisecond)
	client.Notify("msg2")
	client.Notify("msg3")

	ok := waitFor(t, time.Second, func() bool {
		return len(dlqLines(testDLQPath)) > 0
	})

	close(blocked)
	client.Close()
	srv.Close()

	if !ok {
		t.Fatal("expected msg3 to be parked in DLQ but file is empty")
	}
}

// TestClose_DrainsInFlightMessages verifies Close waits for all messages to be sent.
func TestClose_DrainsInFlightMessages(t *testing.T) {
	srv, messages := startServer(t)

	client, _ := New(srv.URL)

	for i := 0; i < 20; i++ {
		client.Notify("msg")
	}

	client.Close() // must block until all 20 are delivered

	if got := len(messages()); got != 20 {
		t.Errorf("expected 20 messages after Close, got %d", got)
	}
}

// TestNotify_HTTPFail verifies failed HTTP deliveries are parked in DLQ.
func TestNotify_HTTPFail(t *testing.T) {
	testDLQPath := "dlq/test_failed.log"
	os.Remove(testDLQPath)
	t.Cleanup(func() { os.Remove(testDLQPath) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	client, _ := New(srv.URL)
	client.dlq.path = testDLQPath
	defer client.Close()

	client.Notify("hello")

	ok := waitFor(t, time.Second, func() bool {
		return len(dlqLines(testDLQPath)) > 0
	})
	if !ok {
		t.Fatal("expected failed message to be parked in DLQ")
	}
}
