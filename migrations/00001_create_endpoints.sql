-- +goose Up
CREATE TABLE endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    sequence BIGINT NOT NULL DEFAULT 0, -- per-endpoint monotonic counter, allocated at ingestion
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF  EXISTS endpoints;