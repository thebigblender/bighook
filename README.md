# bighook

A self-hostable webhook delivery engine in Go. Built around the same architectural primitives as production webhook infrastructure(Svix for the most part).

## Stack

| | |
|---|---|
| Language | Go 1.22+ |
| Database | PostgreSQL 16 |
| Driver | pgx/v5 |
| Query generation | sqlc |
| Migrations | goose |
| Logging | zerolog |

## Setup

**Prerequisites:** Go 1.22+, PostgreSQL 16, goose

```bash
psql -U postgres -c "CREATE USER bighook WITH PASSWORD 'secret';"
psql -U postgres -c "CREATE DATABASE bighook OWNER bighook;"

goose -dir migrations postgres \
  "postgres://bighook:secret@localhost:5432/bighook?sslmode=disable" up

export DATABASE_URL="postgres://bighook:secret@localhost:5432/bighook?sslmode=disable"
go run ./cmd/server
```

## Usage

Register an endpoint:
```bash
psql $DATABASE_URL -c "
  INSERT INTO endpoints (id, url, secret)
  VALUES (gen_random_uuid(), 'https://your-server.com/webhooks', 'your-secret')
  RETURNING id;"
```

Ingest an event:
```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"endpoint_id": "<uuid>", "event_type": "payment.success", "payload": {"amount": 1000}}'
```

Every delivery is signed with `X-Bighook-Signature` (HMAC-SHA256 of `{timestamp}.{payload}`) and `X-Bighook-Timestamp` for replay protection.

## Benchmark

Intel i7-12650H · 16GB RAM · local Postgres · single node

| | bighook | Svix Professional |
|---|---|---|
| Ingestion throughput | 300 req/s | 400 req/s (published cap) |
| p50 latency | 1.7ms | — |
| p95 latency | 3.9ms | — |
| p99 latency | 5.7ms | — |
| Delivery rate | ~160/sec\* | ~20/sec per worker† |
| Success rate | 100% (18k deliveries) | 99.999% SLA |

\* localhost, sub-1ms POST latency to test server
† real network conditions per Svix docs, 40–50ms per POST

## License

MIT