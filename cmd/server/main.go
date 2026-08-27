package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bigblender2115/bighook/internal/config"
	"github.com/bigblender2115/bighook/internal/db"
	"github.com/bigblender2115/bighook/internal/handler"
	"github.com/bigblender2115/bighook/internal/logger"
	"github.com/bigblender2115/bighook/internal/worker"
)

func main() {
	log := logger.New()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := db.InitDB(ctx)
	defer pool.Close()

	queries := db.New(pool)

	workerInstance := worker.NewWorker(pool, queries, log, cfg)
	go workerInstance.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handler.HandleIngestionEvent(pool, queries, log))

	srv := &http.Server{
		Addr:    cfg.Port,
		Handler: mux,
	}

	go func() {
		log.Info().Str("addr", cfg.Port).Msg("server started")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("server shutdown error")
	}

	workerInstance.Wait()

	log.Info().Msg("shutdown complete")
}
