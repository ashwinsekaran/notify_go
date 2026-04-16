package notifier

import (
	"io"
	"net/http"
	"net/http/httptest"
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

// TestNotify_MessageDelivered verifies a sent message is received by the server.
func TestNotify_MessageDelivered(t *testing.T) {
	srv, messages := startServer(t)

	client := New(srv.URL)
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

	client := New(srv.URL)
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

// TestNotify_QueueFull verifies the error handler is called when queue is full.
func TestNotify_QueueFull(t *testing.T) {
	// use a server that blocks forever so workers stay busy and queue fills up
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // hang until we explicitly unblock
	}))

	var mu sync.Mutex
	var dropped []string

	client := New(srv.URL,
		WithQueueSize(1),
		WithWorkers(1),
		WithErrorHandler(func(msg string, err error) {
			mu.Lock()
			dropped = append(dropped, msg)
			mu.Unlock()
		}),
	)

	// first message: worker picks it up and blocks on the server
	// second message: sits in the queue (size=1)
	// third message: queue is full → dropped → error handler called
	client.Notify("msg1")
	time.Sleep(50 * time.Millisecond) // let worker pick up msg1
	client.Notify("msg2")
	client.Notify("msg3") // should be dropped

	ok := waitFor(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(dropped) > 0
	})

	// unblock server first, then close — avoids deadlock
	close(blocked)
	client.Close()
	srv.Close()

	if !ok {
		t.Fatal("expected a dropped message but error handler was never called")
	}
}

// TestClose_DrainsInFlightMessages verifies Close waits for all messages to be sent.
func TestClose_DrainsInFlightMessages(t *testing.T) {
	srv, messages := startServer(t)

	client := New(srv.URL)

	for i := 0; i < 20; i++ {
		client.Notify("msg")
	}

	client.Close() // must block until all 20 are delivered

	if got := len(messages()); got != 20 {
		t.Errorf("expected 20 messages after Close, got %d", got)
	}
}

// TestNotify_ErrorHandler verifies the error handler is called on HTTP failure.
func TestNotify_ErrorHandler(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	var mu sync.Mutex
	var errors []error

	client := New(srv.URL,
		WithErrorHandler(func(msg string, err error) {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}),
	)
	defer client.Close()

	client.Notify("hello")

	ok := waitFor(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(errors) > 0
	})
	if !ok {
		t.Fatal("expected error handler to be called on HTTP 500")
	}
}
