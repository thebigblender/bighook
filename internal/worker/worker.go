package worker

import (
	"bytes"
	"context"
	"net/http"
	"time"

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
	log 	zerolog.Logger
}

func NewWorker(pool *pgxpool.Pool, queries *db.Queries, log zerolog.Logger) *Worker {
	return &Worker{
		pool:    pool,
		queries: queries,
		client:  &http.Client{Timeout: 5 * time.Second},
		log:     log,
	}
}

func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.log.Info().Msg("worker shutting down")
			return
		default:
			w.poll(ctx)
			time.Sleep(5 * time.Second)
		}
	}
}

func backoff(attempt int32) time.Duration {
	return time.Duration(1<<attempt) * time.Second
}

func (w *Worker) poll(ctx context.Context) {
	// pending deliveries
	deliveries, err := w.queries.ClaimPendingDeliveries(ctx)
	if err != nil {
		w.log.Error().Err(err).Msg("failed to claim pending deliveries")
		return
	}

	// for each claimed delivery, "process" it
	for _, delivery := range deliveries {
		w.log.Debug().
			Str("delivery_id", delivery.ID.String()).
			Msg("processing delivery")
		go w.process(ctx, delivery)
	}
}

func (w *Worker) process(ctx context.Context, delivery db.ClaimPendingDeliveriesRow) {
	// 1. fetch event and endpoint for the delivery
	event, err := w.queries.GetEventByID(ctx, delivery.EventID)
	if err != nil {
		w.log.Error().
			Err(err).
			Str("event_id", delivery.EventID.String()).
			Msg("failed to fetch event")
		return
	}

	endpoint, err := w.queries.GetEndpointByID(ctx, delivery.EndpointID)
	if err != nil {
		w.log.
			Error().
			Err(err).
			Str("endpoint_id", delivery.EndpointID.String()).
			Msg("failed to fetch endpoint for delivery")
		return
	}

	// 2. HTTP POST to url with payload, measure latency
	start := time.Now()
	resp, err := w.client.Post(endpoint.Url, "application/json", bytes.NewReader(event.Payload))
	latency := int32(time.Since(start).Milliseconds())

	//nil safe
	var httpStatus pgtype.Int4
	var errMsg pgtype.Text

	if err != nil {
		errMsg = pgtype.Text{String: err.Error(), Valid: true}
	} else {
		httpStatus = pgtype.Int4{Int32: int32(resp.StatusCode), Valid: true}
		resp.Body.Close()
	}

	
	attemptID := pgtype.UUID{Bytes: uuid.New(), Valid: true}
	attemptNumber := delivery.AttemptCount + 1
	isSuccess := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300

	// 3. Update delivery state based on success, retry, or dead letter exhaustion
	if isSuccess {
		if err := w.queries.MarkDelivered(ctx, delivery.ID); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark delivered")
		}

		w.log.Info().
			Str("delivery_id", delivery.ID.String()).
			Str("endpoint_url", endpoint.Url).
			Int32("attempt", attemptNumber).
			Int32("latency_ms", latency).
			Int("http_status", resp.StatusCode).
			Msg("delivery successful")
		
	} else if attemptNumber >= delivery.MaxAttempts {
		if err := w.queries.MarkDead(ctx, delivery.ID); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark dead")
		}

		w.log.Error().
			Str("delivery_id", delivery.ID.String()).
			Msg("delivery exhausted, marking dead")
	} else {
		nextAttempt := time.Now().Add(backoff(delivery.AttemptCount))
		if err := w.queries.MarkFailedWithRetry(ctx, db.MarkFailedWithRetryParams{
			ID:            delivery.ID,
			NextAttemptAt: pgtype.Timestamptz{Time: nextAttempt, Valid: true},
		}); err != nil {
			w.log.Error().Err(err).Str("delivery_id", delivery.ID.String()).Msg("failed to mark failed with retry")
		}

		//doing this weird shit to avoid logging the http status code when err != nil
		warnEvent := w.log.Warn().
			Str("delivery_id", delivery.ID.String()).
			Str("endpoint_url", endpoint.Url).
			Int32("attempt", attemptNumber).
			Int32("latency_ms", latency)

		if err != nil {
			warnEvent.Err(err).Msg("delivery failed with retry")
		} else {
			warnEvent.Int("http_status", resp.StatusCode).Msg("delivery failed with retry")
		}
	}

	// 4. Record delivery attempt
	if err := w.queries.InsertDeliveryAttempt(ctx, db.InsertDeliveryAttemptParams{
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