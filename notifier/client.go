package notifier

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
)

const (
	defaultWorkers   = 10
	defaultQueueSize = 1000
)

// ErrorHandler is called when a notification fails to be delivered.
type ErrorHandler func(message string, err error)

// Config holds optional configuration for a Client.
// Example - (Workers: 10, QueueSize: 1000).
type Config struct {
	Workers   int
	QueueSize int
}

// Client sends HTTP POST notifications to a configured URL.
type Client struct {
	url          string
	httpClient   *http.Client
	queue        chan string
	errorHandler ErrorHandler
	workers      int
	wg           sync.WaitGroup
}

// New creates and starts a Client.
// Optionally pass a Config to override defaults: New(url) or New(url, Config{Workers: 20, QueueSize: 5000}).
// Call Close to drain and shut it down.
func New(url string, cfg ...Config) (*Client, error) {
	if url == "" {
		return nil, fmt.Errorf("url is required")
	}
	workers, queueSize := defaultWorkers, defaultQueueSize
	if len(cfg) > 0 {
		if cfg[0].Workers > 0 {
			workers = cfg[0].Workers
		}
		if cfg[0].QueueSize > 0 {
			queueSize = cfg[0].QueueSize
		}
	}
	c := &Client{
		url:        url,
		httpClient: &http.Client{},
		queue:      make(chan string, queueSize),
		workers:    workers,
		errorHandler: func(msg string, err error) {
			log.Printf("notification error: %v (message: %q)", err, msg)
		},
	}
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
	return c, nil
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
