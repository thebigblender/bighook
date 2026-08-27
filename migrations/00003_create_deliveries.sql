-- +goose Up
CREATE TABLE deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(id),
    endpoint_id UUID NOT NULL REFERENCES endpoints(id),
    status TEXT NOT NULL DEFAULT 'pending', -- pending, in_flight, delivered, dead
    attempt_count INTEGER NOT NULL DEFAULT 0,
    reap_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deliveries_status_next ON deliveries(status, next_attempt_at);
CREATE INDEX idx_deliveries_endpoint_id ON deliveries(endpoint_id);

-- +goose Down
DROP TABLE IF EXISTS deliveries;
