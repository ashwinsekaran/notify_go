package notifier

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	defaultWorkers      = 10
	defaultQueueSize    = 1000
	defaultDLQInterval  = 30 * time.Second
)

// ErrorHandler is called when a notification fails to be delivered.
type ErrorHandler func(message string, err error)

// Config holds optional configuration for a Client.
// Zero values fall back to defaults (Workers: 10, QueueSize: 1000).
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
	dlq          *dlqStore
	done         chan struct{} // signals DLQ worker to stop
	wg           sync.WaitGroup // tracks main workers
	dlqWg        sync.WaitGroup // tracks DLQ worker separately
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
		dlq:        newDLQStore(dlqPath),
		done:       make(chan struct{}),
		errorHandler: func(msg string, err error) {
			log.Printf("notification error: %v (message: %q)", err, msg)
		},
	}

	// start main workers
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	// start DLQ worker
	c.dlqWg.Add(1)
	go c.dlqWorker()

	return c, nil
}

// Notify enqueues a message for delivery. It is non-blocking:
// if the queue is full the message is parked in the DLQ for retry.
func (c *Client) Notify(message string) {
	select {
	case c.queue <- message:
	default:
		// queue full — park in DLQ for retry
		log.Printf("queue full, parking in DLQ: %q", message)
		c.dlq.write(message)
	}
}

// Close stops the DLQ worker, replays any remaining DLQ messages,
// drains all in-flight requests, and releases resources.
func (c *Client) Close() {
	close(c.done)   // 1. stop DLQ worker
	c.dlqWg.Wait() // 2. wait for DLQ worker to fully exit

	// 3. final DLQ drain — blocking send so no messages are lost
	//    workers are still running, they'll drain the queue and make room
	msgs := c.dlq.drain()
	if len(msgs) > 0 {
		log.Printf("dlq: shutdown replay — re-enqueuing %d messages before exit", len(msgs))
		for _, msg := range msgs {
			c.queue <- msg // blocking — waits for a worker to free a slot
		}
	}

	close(c.queue)  // 4. stop main workers — only after DLQ is fully re-enqueued
	c.wg.Wait()    // 5. wait for all main workers to finish
}

// dlqWorker runs in background, replaying failed messages every 30s.
func (c *Client) dlqWorker() {
	defer c.dlqWg.Done()

	ticker := time.NewTicker(defaultDLQInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msgs := c.dlq.drain()
			if len(msgs) == 0 {
				continue
			}
			log.Printf("dlq: scheduled retry — replaying %d messages", len(msgs))
			for _, msg := range msgs {
				select {
				case c.queue <- msg:
				case <-c.done:
					return
				default:
					// queue still full — park back for next retry
					c.dlq.write(msg)
				}
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) worker() {
	defer c.wg.Done()
	for msg := range c.queue {
		if err := c.post(msg); err != nil {
			// HTTP failed — park in DLQ for retry
			log.Printf("delivery failed, parking in DLQ: %v", err)
			c.dlq.write(msg)
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
