package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notify_go/notifier"
)

func main() {
	url := flag.String("url", "", "URL to POST notifications to (required)")
	interval := flag.Duration("interval", 5*time.Second, "Notification send interval")
	flag.DurationVar(interval, "i", 5*time.Second, "Notification send interval (shorthand)")
	flag.Parse()

	if *url == "" {
		fmt.Fprintln(os.Stderr, "usage: notify --url=URL [-i interval]")
		os.Exit(1)
	}

	client, err := notifier.New(*url)
	if err != nil {
		log.Fatalf("notify client - %v", err)
	}
	defer client.Close()

	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var pending []string
	stdinClosed := false

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				lines = nil
				stdinClosed = true
			} else {
				pending = append(pending, line)
			}

		case <-ticker.C:
			for _, msg := range pending {
				client.Notify(msg)
			}
			pending = pending[:0]
			if stdinClosed {
				return
			}

		case <-sigCh:
			log.Println("shutting down...")
			for _, msg := range pending {
				client.Notify(msg)
			}
			return
		}
	}
}
