package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

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
	Status     string `json:"status"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func HandleIngestionEvent(pool *pgxpool.Pool, queries *db.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) //limit to 1mb

		var req IngestionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		req.EndpointID = strings.TrimSpace(req.EndpointID)
		req.EventType = strings.TrimSpace(req.EventType)

		endpointUUID, err := uuid.Parse(req.EndpointID)
		if err != nil || req.EventType == "" || len(req.Payload) == 0 || string(req.Payload) == "null" || !json.Valid(req.Payload) {
			writeError(w, http.StatusBadRequest, "endpoint_id (valid UUID), event_type, and payload (valid JSON) are required")
			return
		}

		// convert uuid.UUID → pgtype.UUID
		pgEndpointID := pgtype.UUID{Bytes: endpointUUID, Valid: true}

		// generate IDs
		eventRaw := uuid.New()
		deliveryRaw := uuid.New()
		pgEventID := pgtype.UUID{Bytes: eventRaw, Valid: true}
		pgDeliveryID := pgtype.UUID{Bytes: deliveryRaw, Valid: true}

		// transaction
		tx, err := pool.Begin(ctx)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		qtx := queries.WithTx(tx)

		// check endpoint exists
		_, err = qtx.GetEndpointByID(ctx, pgEndpointID)
		if err != nil {
			writeError(w, http.StatusNotFound, "endpoint not found")
			return
		}

		// insert event
		err = qtx.InsertEvent(ctx, db.InsertEventParams{
			ID:         pgEventID,
			EndpointID: pgEndpointID,
			EventType:  req.EventType,
			Payload:    []byte(req.Payload),
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to insert event")
			return
		}

		// insert delivery
		err = qtx.InsertDelivery(ctx, db.InsertDeliveryParams{
			ID:         pgDeliveryID,
			EventID:    pgEventID,
			EndpointID: pgEndpointID,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to schedule delivery")
			return
		}

		if err := tx.Commit(ctx); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to commit transaction")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(IngestionResponse{
			EventID:    eventRaw.String(),
			DeliveryID: deliveryRaw.String(),
			Status:     "pending",
		})
	}
}