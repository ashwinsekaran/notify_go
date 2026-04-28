package notifier

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const dlqPath = "dlq/failed.log"

// dlqStore handles reading and writing failed messages to disk.
type dlqStore struct {
	path string
	mu   sync.Mutex // guards file writes — multiple workers can fail at same time
}

func newDLQStore(path string) *dlqStore {
	// create the dlq directory if it doesn't exist
	os.MkdirAll(filepath.Dir(path), 0755)
	return &dlqStore{path: path}
}

// write appends a failed message to the DLQ file.
func (d *dlqStore) write(msg string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(d.path), 0755); err != nil {
		log.Printf("dlq: failed to create directory: %v", err)
		return
	}

	f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("dlq: failed to open file: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, msg)
}

// drain reads all messages from the DLQ file and clears it.
func (d *dlqStore) drain() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.Open(d.path)
	if err != nil {
		return nil // file doesn't exist yet — nothing to replay
	}
	defer f.Close()

	var msgs []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			msgs = append(msgs, line)
		}
	}

	// clear the file after reading
	os.Truncate(d.path, 0)

	return msgs
}
