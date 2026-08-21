-- name: InsertEvent :exec
INSERT INTO events (id, endpoint_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, NOW());

-- name: GetEventByID :one
-- worker needs the payload to POST to the endpoint
SELECT id, endpoint_id, event_type, payload, created_at
FROM events
WHERE id = $1;