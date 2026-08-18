-- +goose Up
CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_number INTEGER NOT NULL,
    http_status INTEGER, --null on network error/timeout
    latency_ms INTEGER NOT NULL,
    error TEXT, --null on success
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_delivery_attempts_delivery_id ON delivery_attempts(delivery_id);

-- +goose Down
DROP TABLE IF EXISTS delivery_attempts;