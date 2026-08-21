package main

import (
	"context"
	"log"
	"net/http"
	"fmt"

	"github.com/bigblender2115/bighook/db"
)

func main() {
	ctx := context.Background()
	pool := initDB(ctx)
	defer pool.Close()

	queries := db.New(pool)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", HandleIngestionEvent(pool, queries))

	worker := NewWorker(pool, queries)
	go worker.Start(ctx)

	fmt.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
