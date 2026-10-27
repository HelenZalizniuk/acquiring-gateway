# Acquiring Gateway

[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/Architecture-Clean%20%7C%20EDA%20%7C%20Saga-1f6feb)](#architecture--core-features)
[![gRPC](https://img.shields.io/badge/RPC-gRPC%20%2B%20Protobuf-244c5a?logo=grpc&logoColor=white)](https://grpc.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Temporal](https://img.shields.io/badge/Temporal-Saga%20Orchestration-000000)](https://temporal.io/)
[![License](https://img.shields.io/badge/License-Proprietary-informational)](#)

High-performance **FinTech acquiring gateway** for card-not-present and merchant payment flows. The platform is built on **Clean Architecture**, **Event-Driven Architecture (EDA)**, and **Temporal Saga orchestration**, with strict transactional integrity, idempotent APIs, and production-grade concurrency controls.

It provides an HTTP edge (API Gateway / Strangler Fig routing), a gRPC payment microservice, a transactional outbox publisher into Apache Kafka, and a Temporal worker that coordinates charge, notification, and compensation steps.

---

## Overview

Merchants submit payments through a stable HTTP contract (`POST /v1/payments`). The gateway routes traffic either to the legacy HTTP stack or to the new gRPC payment service (migrated merchants). The payment service persists the payment and an outbox event in a **single PostgreSQL transaction**, then Temporal orchestrates the saga. An outbox worker publishes confirmed events to Kafka with `acks=all`; consumers process them with **at-least-once** delivery and **event-id idempotency**.

| Concern | Design choice |
|---|---|
| Duplicate merchant retries | Unique `idempotency_key` + lookup-before-insert |
| Dual-write (DB vs broker) | Transactional Outbox (`payments` + `outbox_events`) |
| Cross-step consistency | Temporal Saga with compensating activities |
| Message order | Kafka key = `payment_id` / `aggregate_id` |
| Transient faults | gRPC retry policy + Temporal activity backoff |
| Operability | Graceful shutdown, pprof on `:6060`, structured `slog` |

---

## Key Tech Stack

| Layer | Technologies |
|---|---|
| **Language / frameworks** | Go 1.25, `net/http`, gRPC, Protocol Buffers |
| **Data & messaging** | PostgreSQL 15 (ACID, `pgxpool` connection pooling), Apache Kafka (outbox publisher + consumer group) |
| **Distributed saga** | Temporal Workflow Engine — activities, compensation, retry / timeout policies |
| **Performance & diagnostics** | High-concurrency worker pool, pprof (`:6060`), graceful shutdown (`SIGINT` / `SIGTERM`) |

**Core libraries:** `jackc/pgx/v5`, `segmentio/kafka-go`, `go.temporal.io/sdk`, `google.golang.org/grpc`, `google.golang.org/protobuf`.

---

## Architecture & Core Features

```
                    ┌─────────────────────┐
   Merchants ──HTTP─►  API Gateway :8000  │  Strangler Fig routing
                    └──────────┬──────────┘
               legacy HTTP     │  migrated merchants → gRPC
                 :8080         ▼
                    ┌─────────────────────┐
                    │ Payment Service     │  gRPC :8081
                    │ transport → service │
                    │          → repository
                    └──────────┬──────────┘
                               │  same pgx.Tx
                    ┌──────────▼──────────┐
                    │ PostgreSQL          │  payments + outbox_events
                    └──────────┬──────────┘
           Temporal saga       │           Outbox worker pool
           localhost:7233      │           Kafka acks=all
                    ┌──────────▼──────────┐
                    │ Apache Kafka        │  payments.events
                    └──────────┬──────────┘
                               ▼
                    Idempotent consumer (processed_events)
```

### Storage & transactional integrity

- HTTP payment API with JSON decoding, `201 Created` / `409 Conflict` / `500` mapping.
- **Idempotency-key** uniqueness in PostgreSQL; concurrent duplicates recover via unique-violation handling.
- **Transactional Outbox**: insert into `payments` and `outbox_events` on one `pgx.Tx`. Commit or full rollback — never a payment without an event.

### Clean architecture & distributed saga

- Strict layering: `transport` → `service` → `repository`.
- Gateway talks to the new service over **gRPC** (`PaymentService.ProcessPayment`).
- Temporal workflow `ProcessPaymentWorkflow`: **Charge → Notify → Complete**, with **Compensate** on notification / completion failure (status `REFUNDED`, outbox `PaymentCompensated`).
- Activity timeouts, exponential backoff, and non-retryable `InvalidArgument`.

### Event-driven architecture

- Outbox worker claims `PENDING` rows with `FOR UPDATE SKIP LOCKED`.
- Kafka publisher uses **`RequiredAcks: RequireAll`**, message key = aggregate / payment ID.
- Events are marked processed **only after broker acknowledgement**.
- Consumer group, manual offset commit, **dedup table `processed_events`** on `event_id` for at-least-once duplicates.

### Concurrency & performance tuning

- Channel-based **worker pool** (`jobs`, `processedIDs`) coordinated with `sync.WaitGroup`.
- Parallel Kafka publishes, then **batch** `MarkAsProcessed`.
- Failures isolated via `MarkAsFailed`; `ctx.Done()` stops producers and workers.
- `context.WithoutCancel` used only to flush successful IDs during shutdown.
- Memory / CPU profiling via **pprof** on the outbox worker.

---

## Project Structure

```
acquiring-gateway/
├── cmd/
│   ├── api-gateway/          # HTTP Strangler Fig gateway (:8000)
│   ├── payment-service/      # gRPC payment service + Temporal worker (:8081)
│   ├── outbox-worker/        # Outbox → Kafka publisher + pprof (:6060)
│   └── payment-consumer/     # Kafka consumer + idempotent handler
├── internal/
│   ├── db/                   # PostgreSQL pool (pgxpool)
│   ├── gateway/              # Routing, gRPC client, retry policy
│   ├── paymentsvc/           # New payment bounded context
│   │   ├── repository/       # Persistence + outbox writes
│   │   ├── service/          # Domain / idempotency / compensation
│   │   ├── transport/grpc/   # gRPC adapters
│   │   ├── temporal/         # Workflows, activities, policies
│   │   ├── outbox/           # Publisher, worker pool, claim/mark
│   │   └── kafkaconsumer/    # Consumer, retries, processed_events
│   ├── repository/           # Legacy HTTP payment persistence
│   ├── service/
│   └── transport/http/
├── api/proto/payment/v1/     # payment.proto (source of truth)
├── gen/go/payment/v1/        # Generated gRPC / protobuf stubs
├── migrations/               # SQL schema (payments, outbox, processed_events)
├── scripts/                  # Operational / integration checks
├── docker-compose.yml        # Postgres, Temporal, Kafka
└── Makefile                  # protoc generation
```

---

## How to Run & Test

### Prerequisites

- Go **1.25+**
- Docker / Docker Compose
- `protoc` (only if regenerating stubs: `make proto`)

### Local infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL (`:5432`), Temporal (`:7233`, UI `:8088`), and Kafka (`:9092`).

Apply migrations against the local database:

```bash
psql "postgres://postgres:secretpassword@localhost:5432/acquiring?sslmode=disable" \
  -f migrations/0001_init.sql \
  -f migrations/0002_outbox_kafka.sql
```

> Default `DATABASE_URL` in services is `postgres://postgres:postgres@localhost:5432/acquiring?sslmode=disable`. Override it to match compose credentials, or change compose to match the app default.

### Services

| Process | Command | Port |
|---|---|---|
| API Gateway | `go run ./cmd/api-gateway` | `:8000` |
| Payment service (gRPC + Temporal) | `go run ./cmd/payment-service` | `:8081` |
| Legacy HTTP gateway | `go run .` | `:8080` |
| **Outbox worker** | `go run ./cmd/outbox-worker/main.go` | pprof `:6060` |
| Kafka consumer | `go run ./cmd/payment-consumer` | — |

```bash
export DATABASE_URL="postgres://postgres:secretpassword@localhost:5432/acquiring?sslmode=disable"
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="payments.events"
export TEMPORAL_HOST_PORT="localhost:7233"

go run ./cmd/outbox-worker/main.go
```

### Memory profiling

With the outbox worker running:

```bash
go tool pprof http://localhost:6060/debug/pprof/heap
```

Useful extra endpoints: `/debug/pprof/profile`, `/debug/pprof/goroutine`, `/debug/pprof/allocs`.

### Idempotency check

Requires the HTTP payment API on `:8080` (or set `BASE_URL`):

```bash
./scripts/test_idempotency.sh
```

The script sends two identical `POST /v1/payments` requests: first expects **201 Created**, second expects **409 Conflict**.

### Example payment request (gateway)

```bash
curl -X POST http://localhost:8000/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-Merchant-ID: merchant-qa-001" \
  -d '{
    "merchant_id": "merchant-qa-001",
    "amount": "100.00",
    "currency": "USD",
    "idempotency_key": "demo-001"
  }'
```

---

## Configuration (selected)

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | local Postgres DSN | all data-plane processes |
| `GATEWAY_ADDR` | `:8000` | API Gateway |
| `NEW_PAYMENTS_URL` | `localhost:8081` | Gateway → gRPC |
| `MIGRATED_MERCHANTS` | empty | Strangler Fig allow-list |
| `PAYMENT_GRPC_ADDR` | `:8081` | Payment service |
| `TEMPORAL_HOST_PORT` | `localhost:7233` | Payment service |
| `TEMPORAL_QUEUE` | `payment-saga` | Temporal worker queue |
| `HTTP_ADDR` | `:8080` | Legacy HTTP server |
| `PPROF_ADDR` | `:6060` | Outbox worker |
| `KAFKA_BROKERS` | `localhost:9092` | Outbox worker / consumer |
| `KAFKA_TOPIC` | `payments.events` | Outbox worker / consumer |
| `KAFKA_GROUP_ID` | `payment-event-consumers` | Kafka consumer |
| `OUTBOX_WORKERS` | `8` | Outbox worker pool |
| `OUTBOX_BATCH_SIZE` | `50` | Outbox claim batch |

---

## Roadmap & Planned Enhancements

### Containerization & orchestration

- Multi-stage Dockerfiles per binary (`api-gateway`, `payment-service`, `outbox-worker`, `payment-consumer`)
- Kubernetes Deployments / Services, PDB, resource requests & limits
- Helm charts with environment-specific values
- HTTP / gRPC **liveness and readiness** probes (including Temporal and Kafka connectivity)

### Observability

- Prometheus metrics (request latency, outbox lag, Kafka publish failures, saga compensation rate)
- Grafana dashboards for SLO / error budgets
- OpenTelemetry tracing across HTTP → gRPC → Temporal activities → Kafka

### CI/CD & infrastructure as code

- GitHub Actions: lint, test, proto drift check, image build & scan
- Terraform modules for managed PostgreSQL, Kafka, Temporal, and network policies
- Progressive delivery (canary / merchant-level routing already modeled in the gateway)

---

## License

Proprietary. All rights reserved unless a license file is added to this repository.
