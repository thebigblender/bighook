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
)

type Worker struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	client  *http.Client
}

func NewWorker(pool *pgxpool.Pool, queries *db.Queries) *Worker {
	return &Worker{
		pool:    pool,
		queries: queries,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
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
		return
	}

	// for each claimed delivery, "process" it
	for _, delivery := range deliveries {
		go w.process(ctx, delivery)
	}
}

func (w *Worker) process(ctx context.Context, delivery db.ClaimPendingDeliveriesRow) {
	// 1. fetch event and endpoint for the delivery
	event, err := w.queries.GetEventByID(ctx, delivery.EventID)
	if err != nil {
		return
	}

	endpoint, err := w.queries.GetEndpointByID(ctx, delivery.EndpointID)
	if err != nil {
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

	
	if isSuccess {
		_ = w.queries.MarkDelivered(ctx, delivery.ID)
		_ = w.queries.InsertDeliveryAttempt(ctx, db.InsertDeliveryAttemptParams{
			ID:            attemptID,
			DeliveryID:    delivery.ID,
			AttemptNumber: attemptNumber,
			HttpStatus:    httpStatus,
			LatencyMs:     latency,
			Error:         errMsg,
		})
		return
	}

	
	if attemptNumber >= delivery.MaxAttempts {
		_ = w.queries.MarkDead(ctx, delivery.ID)
	} else {
		nextAttempt := time.Now().Add(backoff(delivery.AttemptCount))
		_ = w.queries.MarkFailedWithRetry(ctx, db.MarkFailedWithRetryParams{
			ID:            delivery.ID,
			NextAttemptAt: pgtype.Timestamptz{Time: nextAttempt, Valid: true},
		})
	}

	_ = w.queries.InsertDeliveryAttempt(ctx, db.InsertDeliveryAttemptParams{
		ID:            attemptID,
		DeliveryID:    delivery.ID,
		AttemptNumber: attemptNumber,
		HttpStatus:    httpStatus,
		LatencyMs:     latency,
		Error:         errMsg,
	})
}