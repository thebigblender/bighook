package main

import (
	"context"
	"net/http"

	"github.com/bigblender2115/bighook/internal/db"
	"github.com/bigblender2115/bighook/internal/handler"
	"github.com/bigblender2115/bighook/internal/logger"
	"github.com/bigblender2115/bighook/internal/worker"
)

func main() {
	log := logger.New()
	ctx := context.Background()
	pool := db.InitDB(ctx)
	defer pool.Close()

	queries := db.New(pool)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handler.HandleIngestionEvent(pool, queries, log))

	workerInstance := worker.NewWorker(pool, queries, log)
	go workerInstance.Start(ctx)

	log.Info().Msg("listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
}
