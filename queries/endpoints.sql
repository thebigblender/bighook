-- name: GetEndpointByID :one
SELECT id, url, secret, created_at
FROM endpoints
WHERE id = $1;

-- name: InsertEndpoint :exec
INSERT INTO endpoints(id, url, secret, created_at)
VALUES(gen_random_uuid(), $1, $2, NOW())
RETURNING *;