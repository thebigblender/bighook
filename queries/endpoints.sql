-- name: GetEndpointByID :one
SELECT id, url, secret, created_at
FROM endpoints
WHERE id = $1;

-- name: AllocateEndpointSequence :one
-- atomically bumps and returns the endpoint's sequence counter; call inside the ingestion tx
UPDATE endpoints
SET sequence = sequence + 1
WHERE id = $1
RETURNING sequence;

-- name: InsertEndpoint :exec
INSERT INTO endpoints(id, url, secret, created_at)
VALUES(gen_random_uuid(), $1, $2, NOW())
RETURNING *;