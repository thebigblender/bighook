package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("Received Webhook on :9000:\n  Headers: %v\n  Body: %s\n", r.Header, string(body))
		
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received": true}`))
	})

	fmt.Println("Mock Target Server listening on :9000/webhook...")
	log.Fatal(http.ListenAndServe(":9000", nil))
}