-- name: InsertDelivery :exec
INSERT INTO deliveries (id, event_id, endpoint_id, status, attempt_count, next_attempt_at, created_at, updated_at)
VALUES ($1, $2, $3, 'pending', 0, NOW(), NOW(), NOW());


-- name: MarkDelivered :exec
UPDATE deliveries
SET status = 'delivered', updated_at = NOW()
WHERE id = $1;


-- name: ClaimPendingDeliveries :many
WITH selected AS (
    SELECT d.id FROM deliveries d
    WHERE d.status = 'pending' AND d.next_attempt_at <= NOW()
    ORDER BY d.next_attempt_at ASC
    LIMIT 50
    FOR UPDATE OF d SKIP LOCKED
)
UPDATE deliveries d
SET status = 'in_flight', updated_at = NOW()
FROM selected, events e, endpoints ep
WHERE d.id = selected.id
  AND e.id = d.event_id
  AND ep.id = d.endpoint_id
RETURNING
    d.id,
    d.event_id,
    d.endpoint_id,
    d.attempt_count,
    d.max_attempts,
    e.payload,
    ep.url,
    ep.secret;


-- name: MarkFailedWithRetry :exec
-- called after a failed attempt
UPDATE deliveries
    SET status = 'pending',
    attempt_count = attempt_count + 1,
    next_attempt_at = $2,
    updated_at = NOW()
WHERE id = $1;


-- name: MarkDead :exec
-- when attempt_count >= max_attempts, no more retries
UPDATE deliveries
    SET status = 'dead',
    attempt_count = attempt_count + 1,
    updated_at = NOW()
WHERE id = $1;

-- name: ReapStuckDeliveries :exec
UPDATE deliveries
SET status = 'pending',
    updated_at = NOW()
WHERE status = 'in_flight'
AND updated_at < NOW() - INTERVAL '10 minutes';