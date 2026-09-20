# Distributed Scheduler

A learning-focused distributed job scheduler written in Go. It accepts immediate jobs over HTTP, stores them in PostgreSQL, and assigns them to connected workers with available capacity. Workers connect to the scheduler through a bidirectional gRPC stream and can be scaled independently.

## Current capabilities

- Accept jobs through `POST /schedule`.
- Persist job state, payload, priority, result, and worker assignment in PostgreSQL.
- Register multiple workers and track their available execution slots in memory.
- Dispatch higher-priority queued jobs to compatible workers.
- Execute these dummy tasks with a random 2–10 second delay:
  - `generate-report`
  - `send-email`
  - `send-notification`
- Requeue unfinished jobs when a worker disconnects cleanly or its connection is detected as closed.
- Scale workers with Docker Compose.

This iteration supports immediate jobs only. Scheduled/cron jobs, automatic retries, worker leases, authentication, and multiple scheduler instances are not implemented yet.

## Architecture

```text
HTTP client
    |
    | POST /schedule
    v
Scheduler (:8080 HTTP, :9090 gRPC) ----> PostgreSQL (:5432)
    ^
    | bidirectional gRPC streams
    +---- Worker 1
    +---- Worker 2
    +---- Worker N
```

Workers initiate the connection to the scheduler. They do not expose an inbound port.

## Project structure

```text
api/proto/                 gRPC service and message definitions
cmd/scheduler/             Scheduler application entry point
cmd/worker/                Worker application entry point
gen/                       Generated Go code from Protocol Buffers
internal/scheduler/        HTTP API, gRPC gateway, coordinator, domain, and store
internal/task/             Shared task names and validation
internal/worker/           Worker task executor
docs/                      Design documents and operating runbook
compose.yaml               Local multi-service environment
Dockerfile                 Scheduler and worker container builds
```

## Run locally with Docker

Start PostgreSQL, one scheduler, and three workers:

```bash
docker compose up -d --build --scale worker=3
```

Submit a job:

```bash
curl -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{
    "task": "send-email",
    "priority": 25,
    "payload": {
      "to": "learner@example.com",
      "subject": "Scheduler test"
    }
  }'
```

Follow the logs:

```bash
docker compose logs -f scheduler worker
```

Stop the system while retaining PostgreSQL data:

```bash
docker compose down
```

## Documentation

- [System design](docs/system-design.md): high-level and low-level architecture.
- [Design decisions](docs/design-decisions.md): reasoning behind concurrency, persistence, and dispatch choices.
- [Runbook](docs/runbook.md): complete startup, debugging, testing, scaling, and shutdown instructions.
