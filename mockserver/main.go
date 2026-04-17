package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var count atomic.Int64

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusInternalServerError)
			return
		}
		n := count.Add(1)
		fmt.Printf("[%s] #%d: %s\n", time.Now().Format("15:04:05"), n, string(body))
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("mock server listening on :%s — waiting for notifications...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
