package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

func verifyRequest(secret string, payload []byte, sig string, tsHeader string) bool {
    ts, err := strconv.ParseInt(tsHeader, 10, 64)
    if err != nil {
        return false
    }
    // reject if older than 5 minutes
    if time.Now().Unix()-ts > 300 {
        return false
    }
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(fmt.Sprintf("%d.", ts)))
    mac.Write(payload)
    expected := hex.EncodeToString(mac.Sum(nil))
    return hmac.Equal([]byte(expected), []byte(sig))
}

func main() {
	http.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("Received Webhook on :9000:\n  Headers: %v\n  Body: %s\n", r.Header, string(body))

		sig := r.Header.Get("X-Bighook-Signature")
		if sig != "" && !verifyRequest("test-secret", body, sig, r.Header.Get("X-Bighook-Timestamp")) && !verifyRequest("", body, sig, r.Header.Get("X-Bighook-Timestamp")) {
			http.Error(w, "Invalid signature", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received": true}`))
	})

	fmt.Println("Mock Target Server listening on :9000/webhook...")
	log.Fatal(http.ListenAndServe(":9000", nil))
}