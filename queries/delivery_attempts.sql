-- name: InsertDeliveryAttempt :exec
INSERT INTO delivery_attempts (id, delivery_id, attempt_number, http_status, latency_ms, error, attempted_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW());