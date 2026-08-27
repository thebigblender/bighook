package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bigblender2115/bighook/internal/config"
	"github.com/bigblender2115/bighook/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type Worker struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	client  *http.Client
	log     zerolog.Logger
	cfg     *config.Config
	wg      sync.WaitGroup
}

func NewWorker(pool *pgxpool.Pool, queries *db.Queries, log zerolog.Logger, cfg *config.Config) *Worker {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = cfg.MaxIdleConns
	t.MaxIdleConnsPerHost = cfg.MaxIdleConnsHost
	t.IdleConnTimeout = 90 * time.Second

	return &Worker{
		pool:    pool,
		queries: queries,
		client:  &http.Client{Timeout: cfg.HTTPTimeout, Transport: t},
		log:     log,
		cfg:     cfg,
	}
}

func (w *Worker) Start(ctx context.Context) {
	reapTicker := time.NewTicker(w.cfg.ReaperInterval)
	defer reapTicker.Stop()

	for i := 0; i < w.cfg.WorkerConcurrency; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			pollTicker := time.NewTicker(w.cfg.PollInterval)
			defer pollTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-pollTicker.C:
					w.poll(ctx)
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			w.log.Info().Msg("worker shutting down")
			return
		case <-reapTicker.C:
			w.reap(ctx)
		}
	}
}

func backoff(attempt int32, maxBackoff time.Duration) time.Duration {
	if attempt > 30 {
		return maxBackoff
	}
	d := time.Duration(1<<attempt) * time.Second
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

func signPayload(secret string, payload []byte, timestamp int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func (w *Worker) reap(ctx context.Context) {
	cutoff := time.Now().Add(-w.cfg.ReaperStaleWindow)
	pgCutoff := pgtype.Timestamptz{Time: cutoff, Valid: true}
	if err := w.queries.ReapStuckDeliveries(ctx, pgCutoff, int32(w.cfg.MaxReaps)); err != nil {
		w.log.Error().Err(err).Msg("reaper failed")
		return
	}
	w.log.Info().Msg("reaper ran")
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) poll(ctx context.Context) {
	deliveries, err := w.queries.ClaimPendingDeliveries(ctx)
	if err != nil {
		w.log.Error().Err(err).Msg("failed to claim pending deliveries")
		return
	}

	for _, delivery := range deliveries {
		w.log.Debug().
			Str("delivery_id", delivery.ID.String()).
			Msg("processing delivery")
		w.wg.Add(1)
		go func(d db.ClaimPendingDeliveriesRow) {
			defer w.wg.Done()
			w.process(d)
		}(delivery)
	}
}

func (w *Worker) process(delivery db.ClaimPendingDeliveriesRow) {
	procCtx, cancel := context.WithTimeout(context.Background(), w.cfg.HTTPTimeout+2*time.Second)
	defer cancel()

	start := time.Now()

	req, err := http.NewRequestWithContext(procCtx, "POST", delivery.Url, bytes.NewReader(delivery.Payload))
	if err != nil {
		w.log.Error().
			Err(err).
			Str("endpoint_url", delivery.Url).
			Msg("failed to create request")
		return
	}

	req.Header.Set("Content-Type", "application/json")
	timestamp := time.Now().Unix()
	req.Header.Set("X-Bighook-Signature", signPayload(delivery.Secret, delivery.Payload, timestamp))
	req.Header.Set("X-Bighook-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Bighook-Sequence", strconv.FormatInt(delivery.Seq, 10))

	resp, err := w.client.Do(req)
	latency := int32(time.Since(start).Milliseconds())

	var httpStatus pgtype.Int4
	var errMsg pgtype.Text

	if err != nil {
		errMsg = pgtype.Text{String: err.Error(), Valid: true}
	} else {
		httpStatus = pgtype.Int4{Int32: int32(resp.StatusCode), Valid: true}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	attemptID := pgtype.UUID{Bytes: uuid.New(), Valid: true}
	attemptNumber := delivery.AttemptCount + 1
	isSuccess := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	if isSuccess {
		if err := w.queries.MarkDelivered(dbCtx, delivery.ID); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark delivered")
		}

		w.log.Info().
			Str("delivery_id", delivery.ID.String()).
			Str("endpoint_url", delivery.Url).
			Int32("attempt", attemptNumber).
			Int32("latency_ms", latency).
			Int("http_status", resp.StatusCode).
			Msg("delivery successful")
	} else if attemptNumber >= delivery.MaxAttempts {
		if err := w.queries.MarkDead(dbCtx, delivery.ID); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark dead")
		}

		w.log.Error().
			Str("delivery_id", delivery.ID.String()).
			Msg("delivery exhausted, marking dead")
	} else {
		nextAttempt := time.Now().Add(backoff(delivery.AttemptCount, w.cfg.MaxBackoff))
		if err := w.queries.MarkFailedWithRetry(dbCtx, db.MarkFailedWithRetryParams{
			ID:            delivery.ID,
			NextAttemptAt: pgtype.Timestamptz{Time: nextAttempt, Valid: true},
		}); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark failed with retry")
		}

		warnEvent := w.log.Warn().
			Str("delivery_id", delivery.ID.String()).
			Str("endpoint_url", delivery.Url).
			Int32("attempt", attemptNumber).
			Int32("latency_ms", latency)

		if err != nil {
			warnEvent.Err(err).Msg("delivery failed with retry")
		} else {
			warnEvent.Int("http_status", resp.StatusCode).Msg("delivery failed with retry")
		}
	}

	if err := w.queries.InsertDeliveryAttempt(dbCtx, db.InsertDeliveryAttemptParams{
		ID:            attemptID,
		DeliveryID:    delivery.ID,
		AttemptNumber: attemptNumber,
		HttpStatus:    httpStatus,
		LatencyMs:     latency,
		Error:         errMsg,
	}); err != nil {
		w.log.Error().Err(err).Str("attempt_id", attemptID.String()).Msg("failed to record delivery attempt")
	} else {
		w.log.Info().
			Str("attempt_id", attemptID.String()).
			Msg("delivery attempt recorded")
	}
}
