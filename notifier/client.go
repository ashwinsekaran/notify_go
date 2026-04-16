package notifier

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
)

const (
	defaultWorkers   = 10
	defaultQueueSize = 1000
)

// ErrorHandler is called when a notification fails to be delivered.
type ErrorHandler func(message string, err error)

// Client sends HTTP POST notifications to a configured URL.
type Client struct {
	url          string
	httpClient   *http.Client
	queue        chan string
	errorHandler ErrorHandler
	workers      int
	wg           sync.WaitGroup
}

// Option configures a Client.
type Option func(*Client)

// WithWorkers sets the number of concurrent HTTP workers (default 10).
func WithWorkers(n int) Option {
	return func(c *Client) { c.workers = n }
}

// WithQueueSize sets the size of the internal message queue (default 1000).
func WithQueueSize(n int) Option {
	return func(c *Client) { c.queue = make(chan string, n) }
}

// WithErrorHandler sets a callback invoked on delivery failures or dropped messages.
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *Client) { c.errorHandler = h }
}

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// New creates and starts a Client. Call Close to drain and shut it down.
func New(url string, opts ...Option) *Client {
	c := &Client{
		url:        url,
		httpClient: &http.Client{},
		queue:      make(chan string, defaultQueueSize),
		workers:    defaultWorkers,
	}
	for _, o := range opts {
		o(c)
	}
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	return c
}

// Notify enqueues a message for delivery. It is non-blocking: if the queue is
// full the message is dropped and the error handler is called (if set).
func (c *Client) Notify(message string) {
	select {
	case c.queue <- message:
	default:
		if c.errorHandler != nil {
			c.errorHandler(message, fmt.Errorf("queue full, message dropped"))
		}
	}
}

// Close stops accepting new messages, waits for all in-flight requests to
// finish, and releases resources.
func (c *Client) Close() {
	close(c.queue)
	c.wg.Wait()
}

func (c *Client) worker() {
	defer c.wg.Done()
	for msg := range c.queue {
		if err := c.post(msg); err != nil && c.errorHandler != nil {
			c.errorHandler(msg, err)
		}
	}
}

func (c *Client) post(message string) error {
	resp, err := c.httpClient.Post(c.url, "text/plain", bytes.NewBufferString(message))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
