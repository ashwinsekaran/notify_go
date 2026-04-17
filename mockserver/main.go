package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Printf("%s - %s\n", time.Now().Format("15:04:05"), string(body))
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("mock server listening on :%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
