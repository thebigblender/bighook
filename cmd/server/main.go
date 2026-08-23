package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bigblender2115/bighook/internal/db"
	"github.com/bigblender2115/bighook/internal/handler"
	"github.com/bigblender2115/bighook/internal/logger"
	"github.com/bigblender2115/bighook/internal/worker"
)

func main() {
	log := logger.New()

	//le signal aware ctx
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	//le db
	pool := db.InitDB(ctx)
	defer pool.Close()

	queries := db.New(pool)

	//le worker
	workerInstance := worker.NewWorker(pool, queries, log)
	go workerInstance.Start(ctx)

	//le server setup
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handler.HandleIngestionEvent(pool, queries, log))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	//le server start
	go func() {
		log.Info().Str("addr", ":8080").Msg("server started")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	//ctx cancelled on signal
	<-ctx.Done()
	log.Info().Msg("shutdown signal received")

	//10 sec for in flight reqs to finish
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("server shutdown error")
	}

	//wait for in flight reqs to finish
	workerInstance.Wait()

	log.Info().Msg("shutdown complete")
}
