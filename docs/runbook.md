# Distributed Scheduler: Local Run and Test Runbook

**Status:** Matches the current implementation
**Last updated:** 2026-09-20

## 1. What this runbook covers

This guide explains how to:

- Start the complete system from scratch.
- Run the scheduler and workers with Docker Compose.
- Run the scheduler and workers locally for debugging.
- Submit and inspect jobs.
- Test queueing, priority, worker capacity, scaling, validation, persistence, and worker disconnection.
- Stop the system while keeping or deleting PostgreSQL data.

## 2. Prerequisites

For the Docker workflow:

- Docker Engine or Docker Desktop.
- Docker Compose v2, invoked as `docker compose`.
- `curl` for HTTP requests.
- `jq` for conveniently reading JSON responses.

For the local Go workflow:

- Go matching the version in `go.mod`.
- PostgreSQL running through Docker or another reachable PostgreSQL server.

`protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` are needed only when changing `worker.proto`. Generated Go files are already committed, so they are not needed merely to run the application.

The default host ports must be available:

| Port | Service |
|---:|---|
| `5432` | PostgreSQL |
| `8080` | Scheduler HTTP API |
| `9090` | Scheduler gRPC worker gateway |

Check them with:

```bash
ss -ltnp | grep -E ':(5432|8080|9090)\b'
```

## 3. Fastest complete startup

From the repository root:

```bash
docker compose up -d --build --scale worker=3
```

This starts:

- One PostgreSQL container.
- One scheduler container.
- Three worker containers.

Check container state:

```bash
docker compose ps
```

Follow scheduler and worker logs:

```bash
docker compose logs -f scheduler worker
```

Press `Ctrl+C` to stop following logs. This does not stop the containers.

Expected scheduler logs include:

```text
scheduler HTTP API listening on :8080
scheduler worker gateway listening on :9090
worker "..." connected with capacity 1 and tasks [generate-report send-email send-notification]
```

## 4. Truly clean startup

Use this when a scenario must not see jobs from earlier runs.

> **Warning:** The first command permanently deletes the local Compose PostgreSQL volume and all jobs stored in it.

```bash
docker compose down --volumes --remove-orphans
docker compose up -d --build --scale worker=3
docker compose ps
```

The scheduler applies all migrations automatically when it starts against the empty database.

If old images are also suspected, rebuild without the Docker layer cache:

```bash
docker compose build --no-cache scheduler worker
docker compose up -d --scale worker=3
```

## 5. Submit one job

```bash
curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{
    "task": "send-email",
    "priority": 25,
    "payload": {
      "to": "learner@example.com",
      "subject": "Scheduler test"
    }
  }' | jq
```

The HTTP response initially reports `queued`. The worker should start it almost immediately if capacity is available. Execution takes a random 2 through 10 seconds.

Capture the job ID in a shell variable:

```bash
JOB_ID=$(
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d '{
      "task": "generate-report",
      "priority": 10,
      "payload": {"report_name": "monthly-sales"}
    }' | jq -r '.job_id'
)

echo "$JOB_ID"
```

## 6. Inspect PostgreSQL

Open an interactive PostgreSQL terminal:

```bash
docker compose exec postgres psql -U scheduler -d scheduler
```

Useful commands inside `psql`:

```sql
\dt
\d jobs

SELECT id, task, priority, status, assigned_worker_id, result, error_message
FROM jobs
ORDER BY created_at DESC;

SELECT status, COUNT(*)
FROM jobs
GROUP BY status
ORDER BY status;

SELECT *
FROM schema_migrations
ORDER BY applied_at;

\q
```

Query one captured job directly from the shell:

```bash
docker compose exec -T postgres \
  psql -U scheduler -d scheduler \
  -x -c "SELECT * FROM jobs WHERE id = '$JOB_ID';"
```

Continuously watch recent jobs:

```bash
watch -n 1 "docker compose exec -T postgres psql -U scheduler -d scheduler -c \"SELECT id, task, priority, status, assigned_worker_id, result FROM jobs ORDER BY created_at DESC LIMIT 10;\""
```

Press `Ctrl+C` to stop `watch`.

## 7. Scenario: successful execution for every task

Start the complete system:

```bash
docker compose up -d --build --scale worker=1
```

Submit each supported task:

```bash
curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"generate-report","priority":10,"payload":{"report":"daily"}}' | jq

curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"send-email","priority":10,"payload":{"to":"test@example.com"}}' | jq

curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"send-notification","priority":10,"payload":{"user_id":"user-1"}}' | jq
```

Watch execution:

```bash
docker compose logs -f scheduler worker
```

Expected final database state for each job:

```text
status = succeeded
result = Job done successfully
```

## 8. Scenario: submit with no workers, then add one

This proves PostgreSQL holds queued jobs until capacity becomes available.

Stop any workers from an earlier run:

```bash
docker compose stop worker
```

Start only PostgreSQL and the scheduler:

```bash
docker compose up -d --build postgres scheduler
```

Submit a job:

```bash
JOB_ID=$(
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d '{"task":"generate-report","priority":10,"payload":{"scenario":"no-worker"}}' \
    | jq -r '.job_id'
)
```

Verify that it remains queued:

```bash
docker compose exec -T postgres \
  psql -U scheduler -d scheduler \
  -tAc "SELECT status FROM jobs WHERE id = '$JOB_ID';"
```

Start one worker:

```bash
docker compose up -d worker
```

After 2 through 10 seconds, query again. The expected state is `succeeded`.

## 9. Scenario: priority ordering

Use one worker with capacity one so only one job can start at a time. A clean database makes the result easiest to see.

```bash
docker compose down --volumes --remove-orphans
docker compose up -d --build postgres scheduler
```

Submit jobs while no worker is running:

```bash
curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"generate-report","priority":1,"payload":{"name":"low"}}' | jq

curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"generate-report","priority":100,"payload":{"name":"high"}}' | jq

curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"generate-report","priority":50,"payload":{"name":"medium"}}' | jq
```

Start one capacity-one worker and follow logs:

```bash
WORKER_CAPACITY=1 docker compose up -d --force-recreate worker
docker compose logs -f scheduler worker
```

Expected assignment order:

1. Priority `100`.
2. Priority `50`.
3. Priority `1`.

For equal priorities, the job with the earlier `created_at` is chosen first.

## 10. Scenario: worker capacity

Create one worker that can execute two jobs concurrently:

```bash
docker compose stop worker
WORKER_CAPACITY=2 docker compose up -d --force-recreate worker
```

Submit three jobs quickly:

```bash
for NUMBER in 1 2 3; do
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d "{\"task\":\"send-notification\",\"priority\":10,\"payload\":{\"number\":$NUMBER}}" \
    | jq -r '.job_id'
done
```

Follow worker logs:

```bash
docker compose logs -f worker
```

Expected behavior:

- Two jobs can log `started` before either one finishes.
- The third job remains queued until one of the first two releases a slot.

## 11. Scenario: multiple workers

Scale the worker service to three processes:

```bash
docker compose up -d --scale worker=3
docker compose ps worker
```

Submit several jobs:

```bash
for NUMBER in 1 2 3 4 5 6; do
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d "{\"task\":\"send-email\",\"priority\":10,\"payload\":{\"number\":$NUMBER}}" \
    | jq -r '.job_id'
done
```

Inspect assignment distribution:

```bash
docker compose logs scheduler | grep 'assigned job'

docker compose exec -T postgres \
  psql -U scheduler -d scheduler \
  -c "SELECT assigned_worker_id, COUNT(*) FROM jobs GROUP BY assigned_worker_id ORDER BY assigned_worker_id;"
```

Worker IDs default to their container hostnames, so scaled replicas register with different IDs.

## 12. Scenario: invalid requests

### Unsupported task

```bash
curl -i -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"unknown-task","priority":1,"payload":{}}'
```

Expected: `400 Bad Request` with an `unsupported task` message.

### Missing task

```bash
curl -i -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"priority":1,"payload":{}}'
```

Expected: `400 Bad Request` with `task is required`.

### Unknown JSON field

```bash
curl -i -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"send-email","priority":1,"payload":{},"unexpected":true}'
```

Expected: `400 Bad Request` because unknown fields are rejected.

### Payload is not an object

```bash
curl -i -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"send-email","priority":1,"payload":[1,2,3]}'
```

Expected: `400 Bad Request` because `payload` must decode into a JSON object.

## 13. Scenario: worker disconnect and requeue

Run one worker with capacity one:

```bash
docker compose stop worker
WORKER_CAPACITY=1 docker compose up -d --force-recreate worker
```

Submit a job and capture its ID:

```bash
JOB_ID=$(
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d '{"task":"generate-report","priority":10,"payload":{"scenario":"disconnect"}}' \
    | jq -r '.job_id'
)
```

Watch until it becomes `running`:

```bash
watch -n 0.2 "docker compose exec -T postgres psql -U scheduler -d scheduler -tAc \"SELECT status FROM jobs WHERE id = '$JOB_ID';\""
```

In another terminal, stop the worker before the 2-through-10-second dummy task completes:

```bash
docker compose stop worker
```

Query the job:

```bash
docker compose exec -T postgres \
  psql -U scheduler -d scheduler \
  -x -c "SELECT id, status, assigned_worker_id FROM jobs WHERE id = '$JOB_ID';"
```

Expected after the scheduler detects the closed stream:

```text
status = queued
assigned_worker_id is null
```

Start the worker again:

```bash
docker compose start worker
```

The job should be assigned again and eventually become `succeeded`.

Because dummy tasks can finish in two seconds, repeat this scenario if the job completes before the worker is stopped.

## 14. Scenario: PostgreSQL persistence across normal shutdown

Stop workers so the new job remains queued:

```bash
docker compose stop worker
```

Submit and capture a job:

```bash
JOB_ID=$(
  curl -sS -X POST http://localhost:8080/schedule \
    -H 'Content-Type: application/json' \
    -d '{"task":"send-email","priority":10,"payload":{"scenario":"restart"}}' \
    | jq -r '.job_id'
)
```

Stop and remove application containers while keeping the named database volume:

```bash
docker compose down
```

Start everything again:

```bash
docker compose up -d --build
```

Query the captured job. It should still exist and should eventually succeed after the worker starts.

This scenario uses `docker compose down` without `--volumes`. Adding `--volumes` would delete the stored job.

## 15. Run locally for development

Use this mode when stepping through Go code or changing it frequently.

First stop Compose versions of the scheduler and worker so ports do not conflict:

```bash
docker compose stop scheduler worker
docker compose up -d postgres
```

Terminal 1 — run the scheduler locally:

```bash
go run ./cmd/scheduler
```

Terminal 2 — run one worker locally:

```bash
WORKER_ID=local-worker-1 \
WORKER_CAPACITY=2 \
WORKER_SCHEDULER_ADDRESS=localhost:9090 \
go run ./cmd/worker
```

Terminal 3 — submit jobs or inspect PostgreSQL:

```bash
curl -sS -X POST http://localhost:8080/schedule \
  -H 'Content-Type: application/json' \
  -d '{"task":"send-notification","priority":10,"payload":{"message":"hello"}}' | jq
```

Stop local processes with `Ctrl+C`, preferably in this order:

1. Worker.
2. Scheduler.
3. PostgreSQL, if no longer needed.

```bash
docker compose stop postgres
```

## 16. Debug with VS Code

Start only PostgreSQL:

```bash
docker compose stop scheduler worker
docker compose up -d postgres
```

Start the existing `Scheduler: debug with PostgreSQL in Docker` launch configuration. Wait for both listener messages before starting the worker.

Run the worker in another terminal:

```bash
WORKER_ID=debug-worker \
WORKER_CAPACITY=1 \
WORKER_SCHEDULER_ADDRESS=localhost:9090 \
go run ./cmd/worker
```

Useful scheduler breakpoints:

| File | Function | What it shows |
|---|---|---|
| `internal/scheduler/httpapi/handler.go` | `schedule` | HTTP decoding and response |
| `internal/scheduler/coordinator/coordinator.go` | `SubmitJob` | Job creation and dispatch signal |
| `internal/scheduler/coordinator/coordinator.go` | `dispatchAvailable` | Worker selection and slot reservation |
| `internal/scheduler/store/postgres/store.go` | `ClaimNextQueuedJob` | Atomic PostgreSQL claim |
| `internal/scheduler/grpcapi/server.go` | `Connect` | Worker registration and stream lifecycle |
| `internal/scheduler/grpcapi/server.go` | `handleWorkerEvent` | Running and terminal job updates |
| `internal/scheduler/coordinator/workers.go` | `HandleJobUpdate` | Persistence and slot release |

Useful worker breakpoints:

| File | Function | What it shows |
|---|---|---|
| `cmd/worker/main.go` | `receiveCommands` | Assignment reception |
| `cmd/worker/main.go` | `executeJobs` | Capacity-limited execution |
| `internal/worker/executor.go` | `Execute` | Task dispatch |
| `internal/worker/executor.go` | `completeDummyTask` | Random delay and cancellation |

Stopping the VS Code debugger stops the local scheduler. Stop the terminal worker with `Ctrl+C`.

Do not pause at startup breakpoints for more than the scheduler's 10-second startup timeout while it is connecting to PostgreSQL and running migrations.

## 17. Stop and reset commands

| Goal | Command | PostgreSQL data retained? |
|---|---|---:|
| Stop worker execution only | `docker compose stop worker` | Yes |
| Stop scheduler only | `docker compose stop scheduler` | Yes |
| Stop all containers | `docker compose stop` | Yes |
| Restart existing containers | `docker compose restart` | Yes |
| Stop and remove containers/network | `docker compose down` | Yes |
| Stop and delete all local database data | `docker compose down --volumes --remove-orphans` | **No** |

A normal end-of-day shutdown is:

```bash
docker compose down
```

A complete destructive reset is:

```bash
docker compose down --volumes --remove-orphans
```

## 18. Troubleshooting

### `address already in use`

Another local process or container owns one of the ports.

```bash
ss -ltnp | grep -E ':(5432|8080|9090)\b'
docker compose ps
```

Do not run the Compose scheduler and a local scheduler simultaneously with the default ports.

### Scheduler reports `context deadline exceeded` during debugging

The scheduler gives startup database work 10 seconds. Pausing after that context is created can let the deadline expire before `Ping` or migrations finish. Restart debugging and avoid pausing until startup completes.

### Jobs stay queued

Check:

```bash
docker compose ps worker
docker compose logs scheduler worker
```

Common causes:

- No worker is connected.
- All worker slots are busy.
- The job task is not supported by the connected workers.
- The dispatcher logged a PostgreSQL error.

### Worker repeatedly restarts

Inspect its logs:

```bash
docker compose logs --tail=100 worker
```

Confirm the scheduler is listening and the worker uses `scheduler:9090` inside Compose.

### PostgreSQL is healthy but the local scheduler cannot connect

Check the published port:

```bash
docker compose ps postgres
```

The local scheduler should use:

```text
postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable
```

The scheduler container uses hostname `postgres` instead of `localhost` because it connects over the Compose network.

## 19. Known test boundaries

- Heartbeats are sent but timeout-based worker eviction is not implemented.
- A hard scheduler crash can leave `assigned` or `running` jobs without lease recovery.
- Disconnect-and-requeue behavior can cause a job to execute more than once.
- There is no `GET /jobs/{id}` endpoint yet, so this runbook inspects status through PostgreSQL.
- The dummy executor always succeeds unless its context is cancelled, so the `failed` path requires a future failing task or a focused automated test.
