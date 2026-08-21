package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/bigblender2115/bighook/internal/db"
	"github.com/bigblender2115/bighook/internal/handler"
	"github.com/bigblender2115/bighook/internal/worker"
)

func main() {
	ctx := context.Background()
	pool := db.InitDB(ctx)
	defer pool.Close()

	queries := db.New(pool)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handler.HandleIngestionEvent(pool, queries))

	workerInstance := worker.NewWorker(pool, queries)
	go workerInstance.Start(ctx)

	fmt.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
