package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/bigblender2115/bighook/internal/db"
)

type IngestionRequest struct {
	EndpointID string          `json:"endpoint_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
}

type IngestionResponse struct {
	EventID    string `json:"event_id"`
	DeliveryID string `json:"delivery_id"`
	Sequence   int64  `json:"sequence"`
	Status     string `json:"status"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func sendJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func HandleIngestionEvent(pool *pgxpool.Pool, queries *db.Queries, log zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		var req IngestionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Warn().Err(err).Msg("failed to decode request body")
			sendJSONError(w, "invalid JSON or body exceeds 1MB", http.StatusBadRequest)
			return
		}

		req.EndpointID = strings.TrimSpace(req.EndpointID)
		req.EventType = strings.TrimSpace(req.EventType)

		endpointUUID, err := uuid.Parse(req.EndpointID)
		if err != nil || req.EventType == "" || len(req.Payload) == 0 || string(req.Payload) == "null" || !json.Valid(req.Payload) {
			log.Warn().
				Str("endpoint_id", req.EndpointID).
				Str("event_type", req.EventType).
				Msg("request validation failed")
			sendJSONError(w, "validation failed", http.StatusBadRequest)
			return
		}

		pgEndpointID := pgtype.UUID{Bytes: endpointUUID, Valid: true}

		eventRaw := uuid.New()
		deliveryRaw := uuid.New()
		pgEventID := pgtype.UUID{Bytes: eventRaw, Valid: true}
		pgDeliveryID := pgtype.UUID{Bytes: deliveryRaw, Valid: true}

		reqLog := log.With().
			Str("endpoint_id", endpointUUID.String()).
			Str("event_id", eventRaw.String()).
			Str("delivery_id", deliveryRaw.String()).
			Str("event_type", req.EventType).
			Logger()

		tx, err := pool.Begin(ctx)
		if err != nil {
			reqLog.Error().Err(err).Msg("failed to start transaction")
			sendJSONError(w, "internal server error", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(ctx)

		qtx := queries.WithTx(tx)

		// allocate the delivery's per-endpoint sequence number inside the tx
		seq, err := qtx.AllocateEndpointSequence(ctx, pgEndpointID)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign key violation
				reqLog.Warn().Msg("endpoint not found")
				sendJSONError(w, "endpoint not found", http.StatusNotFound)
			} else {
				reqLog.Error().Err(err).Msg("failed to allocate sequence")
				sendJSONError(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}

		err = qtx.InsertEvent(ctx, db.InsertEventParams{
			ID:         pgEventID,
			EndpointID: pgEndpointID,
			EventType:  req.EventType,
			Payload:    []byte(req.Payload),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign key violation
				reqLog.Warn().Msg("endpoint not found")
				sendJSONError(w, "endpoint not found", http.StatusNotFound)
			} else {
				reqLog.Error().Err(err).Msg("failed to insert event")
				sendJSONError(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}

		err = qtx.InsertDelivery(ctx, db.InsertDeliveryParams{
			ID:         pgDeliveryID,
			EventID:    pgEventID,
			EndpointID: pgEndpointID,
			Seq:        seq,
		})
		if err != nil {
			reqLog.Error().Err(err).Msg("failed to schedule delivery")
			sendJSONError(w, "internal server error", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			reqLog.Error().Err(err).Msg("failed to commit ingestion transaction")
			sendJSONError(w, "internal server error", http.StatusInternalServerError)
			return
		}

		reqLog.Info().
			Int("payload_bytes", len(req.Payload)).
			Msg("event ingested successfully")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(IngestionResponse{
			EventID:    eventRaw.String(),
			DeliveryID: deliveryRaw.String(),
			Sequence:   seq,
			Status:     "pending",
		})
	}
}
