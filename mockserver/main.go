package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Printf("%s - %s\n", time.Now().Format("15:04:05"), string(body))
		w.WriteHeader(http.StatusOK)
	})

	log.Println("mock server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
