# Distributed Scheduler: Design Decisions and Rationale

**Format:** Confluence-ready Markdown
**Status:** Decisions reflected in the current implementation
**Last updated:** 2026-09-20

## 1. Purpose of this page

This page explains why the system was designed the way it is. The companion [High-Level and Low-Level Design](system-design.md) describes what the components do and how data moves through them. Operational steps and test scenarios are in the [Local Run and Test Runbook](runbook.md).

Each section records:

- The problem being solved.
- The selected design.
- Why it was selected.
- What it does and does not guarantee.
- The tradeoffs or future work it creates.

## 2. Decision summary

| # | Decision | Main reason |
|---:|---|---|
| 1 | Separate scheduler and worker executables | Independent deployment and scaling |
| 2 | Keep both executables in one repository | Shared contract and simpler development |
| 3 | Scheduler hosts the gRPC server | Workers can connect outbound without inbound discovery |
| 4 | Use one bidirectional gRPC stream per worker | Assignments and results share one long-lived connection |
| 5 | Store jobs in PostgreSQL | Jobs must survive process restarts |
| 6 | Keep live workers in memory | Connection state is temporary and process-local |
| 7 | Protect the registry with `sync.RWMutex` | Go maps and mutable session data are not safe for concurrent access |
| 8 | Use a single dispatch loop | Serialize in-process dispatch decisions |
| 9 | Coalesce dispatch signals | Avoid blocking request handlers and redundant dispatch passes |
| 10 | Reserve a worker slot before claiming a job | Prevent over-assignment |
| 11 | Use `FOR UPDATE SKIP LOCKED` | Atomically claim a job without duplicate concurrent claims |
| 12 | Use conditional state updates | Reject stale workers and invalid transitions |
| 13 | Requeue work on stream disconnection | Recover capacity after ordinary worker loss |
| 14 | Size assignment channels to worker capacity | Bound queued delivery and provide backpressure |
| 15 | Use one sender per gRPC stream | Preserve stream send safety and message ordering |
| 16 | Use a fixed worker goroutine pool | Enforce advertised capacity locally |
| 17 | Use contexts throughout | Propagate cancellation and shutdown |
| 18 | Use JSONB and `google.protobuf.Struct` for payloads | Allow different task payload shapes |
| 19 | Validate a fixed set of task names | Prevent permanently unexecutable jobs |
| 20 | Use embedded, locked database migrations | Reproducible startup without concurrent migration races |
| 21 | Use a PostgreSQL connection pool | Support concurrent HTTP, dispatch, and gRPC database operations |
| 22 | Use priority then FIFO ordering | Predictable scheduling among queued jobs |
| 23 | Do not give workers their own database | Workers are stateless execution capacity |
| 24 | Use multi-stage, non-root containers | Small runtime images with reduced privileges |
| 25 | Start with immediate jobs only | Keep the first scheduling problem narrow |

## 3. Scheduler and workers are separate executables

### Problem

The scheduler and workers have different responsibilities and scaling characteristics. The scheduler accepts jobs and coordinates state. Workers consume CPU, network, or task-specific resources while executing jobs.

### Decision

Build two binaries:

- `cmd/scheduler`
- `cmd/worker`

### Why

Workers can be added or removed without restarting the scheduler. A developer can run one scheduler with one worker locally, while production can run many workers. Worker crashes do not directly crash the scheduler process.

### Tradeoff

The system now has a network boundary. Contracts, connection handling, partial failures, and deployment configuration must be managed explicitly.

## 4. One repository, separate runtime systems

### Problem

Separate processes do not necessarily require separate repositories. Splitting repositories immediately would add release and version coordination before the protocol has stabilized.

### Decision

Keep scheduler code, worker code, the protobuf contract, and generated bindings in one Go module.

### Why

- Contract changes can update both consumers in one change.
- Shared task-name types prevent string drift.
- Local builds and Docker Compose remain simple.
- The runtime boundary is preserved because there are still two binaries and two processes.

### Tradeoff

A later organization may choose separate repositories and publish the protobuf package as a versioned module. The current package structure does not prevent that migration.

## 5. The scheduler hosts gRPC; workers connect outbound

### Problem

If the scheduler called an inbound endpoint on every worker, it would need worker addresses, per-worker ports, service discovery, firewall rules, and reachability into dynamically scaled worker environments.

### Decision

The scheduler listens on port `9090`. Every worker opens an outbound `Connect` stream to it.

### Why

- Outbound worker connections work well with Docker, Kubernetes, NAT, and firewalls.
- Workers need no exposed port.
- The open stream itself proves that a worker has an active communication path.
- Removing a worker closes its stream, which gives the scheduler a lifecycle event.

### Tradeoff

The scheduler owns many long-lived connections. It must manage connection cleanup, stream backpressure, and eventual reconnect behavior.

## 6. Bidirectional streaming instead of unary RPC calls

### Problem

The scheduler must send assignments to workers, while workers must send registration, heartbeats, and results back to the scheduler.

### Decision

Use one bidirectional streaming RPC:

```protobuf
rpc Connect(stream WorkerEvent) returns (stream CoordinatorCommand);
```

### Why

- Both sides can send messages whenever necessary.
- The scheduler does not repeatedly open a new RPC for every job.
- Worker presence is naturally tied to stream lifetime.
- Heartbeats, progress, results, and future commands can share the same connection.

### Tradeoff

Streaming code has more concurrency concerns than unary request/response code. The implementation must serialize sends and respond correctly when either side disappears.

## 7. PostgreSQL is the durable job source

### Problem

Jobs must not vanish when the scheduler restarts. An in-memory queue would lose all pending work.

### Decision

Insert every accepted job into PostgreSQL with status `queued` before requesting dispatch.

### Why

- PostgreSQL provides durable storage and transactions.
- Queued jobs survive scheduler restarts.
- Status, assignment, result, and error are available for audit and future APIs.
- Database locking can coordinate concurrent claimers.

### Guarantee

A `202 Accepted` response means the job was inserted successfully. It does not mean a worker has already completed or even accepted it.

### Tradeoff

The database is in the scheduling path, so database latency and availability affect submissions and dispatch.

## 8. Workers do not have their own database

### Problem

Giving every worker a database introduces data ownership questions and synchronization between worker state and scheduler state.

### Decision

Workers are stateless executors. PostgreSQL belongs to the scheduler system.

### Why

- Workers can be created and destroyed freely.
- Scaling does not create more databases or schemas.
- The scheduler remains the authority for job state.
- A worker only needs the assignment payload and a way to report its result.

### Tradeoff

Large task-specific artifacts may eventually need object storage or another external system. That is different from giving each worker a scheduling database.

## 9. Live workers are stored in memory

### Problem

The scheduler needs to know which workers are connected right now and how to send commands to their active streams.

### Decision

Maintain an in-memory registry keyed by worker ID.

### Why

A database row cannot contain a live Go channel or gRPC stream. Connection presence, assignment channels, and current in-process slot reservations are ephemeral runtime state.

Persisting a worker row would not prove that the worker is currently reachable. The active stream is the useful fact.

### Tradeoff

The registry disappears when the scheduler process exits. A multi-scheduler deployment needs an explicit ownership and routing design because one scheduler process cannot send through another process's gRPC stream.

## 10. `sync.RWMutex` protects the worker registry

### Problem

Go's HTTP server, gRPC server, and dispatcher run in different goroutines. Registration, disconnection, status updates, and dispatch can touch the worker map and session counters concurrently.

Go maps are not safe for concurrent read and write. A data race could also allow two goroutines to use the same last worker slot.

### Decision

Protect the registry map and mutable worker-session fields with `sync.RWMutex`.

### Why

- `Lock` gives exclusive access for registration, removal, slot reservation, and completion.
- `RLock` allows concurrent read-only snapshots.
- Capacity checks and increments happen in one critical section.

In Go, the relevant unit is normally a goroutine. Goroutines may execute on different operating-system threads, but the mutex protects the shared memory regardless of which thread runs them.

### Important boundary

This mutex protects only one scheduler process. It cannot coordinate two scheduler replicas on different machines. PostgreSQL transactions and locks are required for cross-process database coordination.

## 11. Copies are returned instead of internal slices

### Problem

A slice contains a pointer to an underlying array. Returning the registry's internal `SupportedTasks` slice would allow another goroutine to modify registry-owned memory without holding the mutex.

### Decision

Copy supported-task slices when storing or returning worker snapshots.

### Why

The caller owns the copy and cannot mutate registry internals accidentally. The mutex then has a clear ownership boundary.

### Tradeoff

Copying allocates a small amount of memory. The task lists are short, so safety is more valuable than avoiding this small cost.

## 12. A single event-driven dispatch loop

### Problem

Jobs can become dispatchable because of several concurrent events:

- A new job is submitted.
- A worker registers.
- A job completes and frees a slot.
- A worker disconnects and its jobs are requeued.

Allowing every handler to run its own full dispatch loop would create unnecessary in-process contention and make capacity reasoning harder.

### Decision

Only `Coordinator.Run` executes `dispatchAvailable`. Other goroutines send a dispatch request through a channel.

### Why

- Dispatch decisions within one process are serialized.
- HTTP and gRPC handlers stay short.
- Capacity and queue-draining behavior are easier to debug.
- PostgreSQL still provides protection if another database client claims jobs concurrently.

### Tradeoff

One dispatch loop limits the rate at which a single scheduler issues assignments. This is appropriate for the current system and can be measured before adding complexity.

## 13. Dispatch requests use a capacity-one buffered channel

### Problem

Many jobs can complete at nearly the same time. Sending one blocking dispatch request per event could block request handlers or create a large event backlog even though one dispatch pass drains all available work.

### Decision

Use `make(chan struct{}, 1)` and a non-blocking send.

### Why

The channel represents “dispatch work may now be possible,” not a count of jobs. Once one signal is pending, additional identical signals provide no extra information.

### Tradeoff

This pattern is correct only because a dispatch pass loops until no more assignments can be made. If dispatch were changed to handle exactly one job, coalescing signals could strand work.

## 14. Do not hold the registry mutex during database calls

### Problem

Database I/O can take milliseconds or time out. Holding a mutex across that call would prevent registrations, disconnections, and job completions from updating the registry.

### Decision

Take a worker snapshot, reserve a short-lived slot under the mutex, release the mutex, and then claim the database job.

### Why

Critical sections remain small and predictable. Other goroutines can continue accessing unrelated worker sessions while PostgreSQL is working.

### Tradeoff

State can change after the snapshot. Every later operation therefore rechecks that the worker still exists. If delivery fails, the claimed job is requeued and the reserved slot is released.

## 15. Reserve capacity before claiming a job

### Problem

If the scheduler claimed a job first and only then checked worker capacity, it could move a job to `assigned` without having a slot available to deliver it.

### Decision

Increment the worker's `ActiveJobs` count under the registry mutex before asking PostgreSQL for a compatible job.

### Why

The reservation prevents another scheduling path from taking the same slot. Failure paths explicitly release the reservation.

### Tradeoff

The reservation is process-local and temporary. PostgreSQL stores the durable assignment after a job is claimed.

## 16. Atomic job claiming with `SELECT ... FOR UPDATE SKIP LOCKED`

### Problem

Two concurrent dispatchers could both read the same `queued` row and assign it twice if selection and assignment were separate unprotected operations.

### Decision

Select and update the candidate in one PostgreSQL statement:

```sql
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE status = 'queued' AND task = ANY($1)
    ORDER BY priority DESC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status = 'assigned', assigned_worker_id = $2, updated_at = NOW()
WHERE id = (SELECT id FROM candidate)
RETURNING ...;
```

### Why `FOR UPDATE`

`FOR UPDATE` places a row-level lock on the selected candidate for the transaction. Another transaction cannot simultaneously treat that locked row as its own candidate.

### Why `SKIP LOCKED`

Without `SKIP LOCKED`, another dispatcher can wait behind the locked first row even when other queued jobs exist. `SKIP LOCKED` tells it to move past rows currently claimed by another transaction.

### Why the CTE and `UPDATE` are one statement

PostgreSQL runs the statement atomically in one transaction. There is no application-level gap between reading the candidate and changing it to `assigned`.

### Important boundary

The row lock prevents simultaneous database claims. It does not provide exactly-once task execution after network failures. Leases and idempotent task handling are still needed for that.

## 17. Database locking and Go mutexes solve different problems

| Mechanism | Protects | Scope | Example |
|---|---|---|---|
| `sync.RWMutex` | Go maps, counters, channels, job-ID sets | One scheduler process | Prevent two goroutines from consuming one worker slot |
| PostgreSQL row lock | Job rows | All clients of that database | Prevent two dispatchers from claiming one queued job |
| PostgreSQL advisory migration lock | Migration execution | All scheduler processes using the database | Prevent concurrent migration runs |

Using only a mutex would fail when there are multiple scheduler processes. Using only database locks would not make an in-memory Go map safe.

## 18. Priority ordering with FIFO tie-breaking

### Problem

The scheduler needs deterministic ordering when multiple jobs are queued.

### Decision

Order candidates by:

```sql
ORDER BY priority DESC, created_at ASC
```

### Why

Higher numeric priority runs first. Jobs with equal priority run in creation order, which is easy for users to understand.

### Tradeoff

A constant stream of high-priority jobs can starve low-priority jobs. Aging or weighted fairness can be introduced later if measurements show starvation.

## 19. Partial index for queued jobs

### Problem

The claim query repeatedly filters for `status = 'queued'` and orders by priority and creation time. Finished jobs should not make this lookup progressively slower.

### Decision

Create a partial index containing only queued jobs:

```sql
CREATE INDEX jobs_queued_dispatch_idx
    ON jobs (priority DESC, created_at ASC)
    WHERE status = 'queued';
```

### Why

The index matches the filter and ordering used by dispatch. Completed history does not occupy this scheduling index.

### Tradeoff

Every transition into or out of `queued` updates the index. That is the intended cost for faster queue access.

## 20. Conditional job state updates

### Problem

A delayed, duplicated, or incorrect worker update must not overwrite a newer job state or update a job assigned to another worker.

### Decision

The update includes conditions for:

- Job ID.
- Assigned worker ID.
- Allowed previous state.

The store checks `RowsAffected()` and rejects an update that changed zero rows.

### Why

The database performs the validation and update atomically. Application code does not read a state, pause, and then update based on a stale value.

### Tradeoff

Workers must treat rejected transitions as protocol or ownership errors. Future retries may require event IDs or attempt numbers for safe deduplication.

## 21. Persist `assigned_worker_id`

### Problem

The scheduler needs to know which worker owns the current execution attempt and must reject updates from other workers.

### Decision

Write the worker ID into the job row when changing `queued` to `assigned`.

### Why

- State updates can verify ownership.
- Disconnect cleanup can requeue all unfinished jobs for one worker.
- Operators can inspect where a job ran.

### Tradeoff

A worker ID identifies a process session only informally in the current version. Attempt IDs or session generations would make ownership stronger across reconnects.

## 22. Requeue on worker disconnection

### Problem

If a worker disappears while holding jobs, those jobs must not remain permanently assigned to unavailable capacity.

### Decision

When its gRPC stream ends, remove the worker session and update its `assigned` and `running` jobs back to `queued`.

### Why

Another live worker can pick up the unfinished work. This provides basic recovery for detected worker failures.

### Tradeoff and delivery semantics

The old worker may have completed an external side effect just before the connection was lost. Requeuing can execute the job again. The current system therefore provides at-least-once behavior in failure scenarios, not exactly-once behavior.

Real task handlers should be idempotent, or the protocol should add attempt IDs and application-specific deduplication.

## 23. Assignment channels are bounded by capacity

### Problem

An unbounded in-memory queue per worker could grow even when the worker cannot execute more work.

### Decision

Create the scheduler-side assignment channel with buffer size equal to worker capacity.

### Why

- Memory use is bounded.
- The channel represents the worker's advertised number of execution slots.
- A full channel is treated as delivery failure instead of silently accumulating work.

### Tradeoff

Capacity is trusted information supplied by the worker. Authentication and policy limits are needed before accepting arbitrary values in an untrusted environment.

## 24. One goroutine sends on each gRPC stream

### Problem

Assignments, heartbeats, and job results can originate from multiple goroutines. Concurrent calls to stream `Send` complicate ordering and are not the supported usage pattern for multiple senders.

### Decision

- On the scheduler, the `Connect` handler goroutine sends assignments.
- On the worker, execution goroutines send events into a channel, and the main loop performs the actual gRPC sends.

### Why

All outbound messages are serialized. This avoids a send mutex, preserves message ordering, and keeps stream ownership clear.

### Tradeoff

A slow network can delay other outbound messages on the same stream. Buffer sizes and timeouts may need tuning after measurement.

## 25. Fixed worker goroutine pool and `sync.WaitGroup`

### Problem

Starting an unrestricted goroutine for every assignment would allow the worker to exceed its advertised capacity.

### Decision

Start exactly `WORKER_CAPACITY` execution goroutines. Each reads from the shared assignment channel. Track them with `sync.WaitGroup`.

### Why

- Capacity is enforced inside the worker as well as the scheduler.
- Concurrency is bounded and predictable.
- `WaitGroup` allows shutdown to wait until execution goroutines observe cancellation and exit.

### Tradeoff

All task types currently share one capacity pool. Future workloads may need separate CPU, memory, or task-specific limits.

## 26. Contexts for cancellation and deadlines

### Problem

Database calls, network streams, timers, and goroutines must stop when a request is cancelled or the process shuts down.

### Decision

Pass `context.Context` through HTTP, coordinator, store, gRPC, and executor operations.

### Why

- Startup database work has a 10-second deadline.
- HTTP request cancellation reaches job submission.
- Process signals cancel coordinator and worker loops.
- Dummy task timers stop early when the worker shuts down.
- Worker cleanup has a bounded timeout.

### Tradeoff

Context cancellation is cooperative. Every blocking operation must select on the context or call an API that accepts it.

## 27. `pgxpool.Pool` instead of one database connection

### Problem

HTTP submissions, dispatch claims, worker updates, and cleanup can reach PostgreSQL concurrently. A single connection would serialize unrelated work and become a bottleneck.

### Decision

Use `pgxpool.Pool`.

### Why

The pool is safe for concurrent use, reuses connections, and lets independent database operations proceed concurrently up to configured limits.

### Tradeoff

Default pool settings are currently used. Production deployment should configure connection limits relative to PostgreSQL capacity and scheduler replica count.

## 28. Embedded migrations and a PostgreSQL advisory lock

### Problem

Every environment needs the same schema. Two scheduler processes starting together must not apply the same migrations concurrently.

### Decision

- Embed SQL migration files into the scheduler binary.
- Record applied filenames in `schema_migrations`.
- Acquire `pg_advisory_xact_lock` inside the migration transaction.

### Why

- The binary contains the schema changes it expects.
- Startup can initialize an empty database automatically.
- The advisory lock serializes migration execution across processes.
- A transaction keeps each migration batch atomic.

### Tradeoff

Application-managed migrations couple startup permission to schema-change permission. Larger production environments may move migration execution to a separate deployment step.

## 29. JSONB for stored payloads

### Problem

Each task has a different input shape. Creating a database table or fixed column set for every dummy task would couple scheduling infrastructure to task-specific fields.

### Decision

Store payloads as a JSON object in a `JSONB` column.

### Why

- The scheduler can transport task inputs without understanding every field.
- PostgreSQL validates that the value is JSON.
- Payloads remain inspectable.
- New task fields do not require immediate schema migrations.

### Tradeoff

The database does not strongly type task-specific fields. Each real task handler should validate its payload before performing side effects.

## 30. `google.protobuf.Struct` on the gRPC boundary

### Problem

The flexible JSON payload must cross a typed Protocol Buffers contract.

### Decision

Represent it as `google.protobuf.Struct` in `JobAssignment`.

### Why

`Struct` maps naturally to a JSON object and allows the worker to reconstruct `map[string]any`.

### Tradeoff

The protobuf compiler cannot validate task-specific payload fields. Once task schemas stabilize, typed protobuf messages or a versioned payload envelope may provide stronger compatibility.

## 31. Fixed task names and protobuf enums

### Problem

Accepting any task string would create jobs that no worker can execute. Repeating raw strings across packages can also produce spelling drift.

### Decision

- Validate HTTP task names against the shared `internal/task` package.
- Represent task types as an enum in protobuf.
- Reject workers that advertise an unknown or empty task set.

### Why

Only executable work enters the durable queue, and unsupported wire values fail explicitly.

### Tradeoff

Adding a task requires updating shared task constants, the protobuf enum, conversion code, and worker execution code. That explicit work is useful because it forces compatibility to be considered.

## 32. Strict HTTP decoding and a 1 MiB limit

### Problem

Silently accepting misspelled fields or unbounded request bodies makes client errors difficult to diagnose and exposes unnecessary memory risk.

### Decision

- Limit the body to 1 MiB.
- Reject unknown fields.
- Require exactly one JSON value.
- Return client-safe validation messages.

### Why

API mistakes fail early, before a durable job is created. The size limit bounds memory consumed during decoding.

### Tradeoff

Strict unknown-field rejection requires deliberate API evolution when adding or renaming fields.

## 33. HTTP for users and gRPC for workers

### Problem

End users and internal worker processes have different interface needs.

### Decision

- Use HTTP/JSON on port `8080` for job submission.
- Use gRPC/Protocol Buffers on port `9090` for worker communication.

### Why

HTTP/JSON is easy to call from browsers, scripts, and external services. gRPC provides generated types and efficient long-lived bidirectional streams for controlled internal clients.

### Tradeoff

The scheduler operates two network servers and two API formats. Shared domain types and explicit transport adapters prevent transport concerns from leaking into the coordinator.

## 34. Dummy task implementations live only in the worker

### Problem

The scheduler should coordinate work without importing or running task-specific behavior.

### Decision

Put `GenerateReport`, `SendEmail`, and `SendNotification` behind the worker's `Executor`.

### Why

- Scheduler deployments do not need task dependencies.
- Task execution cannot block scheduler request handling.
- Worker images can evolve toward task-specific variants.
- The executor has one responsibility: select and run an implementation.

### Tradeoff

The scheduler must rely on the worker's advertised supported-task list and validate protocol results.

## 35. Multi-stage Docker builds and `scratch` runtime images

### Problem

Shipping the Go toolchain in runtime containers increases image size and attack surface.

### Decision

Compile static scheduler and worker binaries in a Go build stage, then copy each into a separate `scratch` target.

### Why

- Runtime images contain only the application binary.
- Scheduler and worker targets can be built from one Dockerfile.
- Containers run as numeric non-root user `65532`.

### Tradeoff

`scratch` has no shell or debugging utilities. Troubleshooting uses logs, external tools, or a separate debug image.

## 36. Hostname as the default worker ID

### Problem

Scaled Docker workers need distinct IDs without manually assigning one to every replica.

### Decision

Use `WORKER_ID` when provided; otherwise use the operating-system hostname.

### Why

Docker gives each replica a distinct container hostname, so `docker compose --scale worker=N` works without per-replica configuration.

### Tradeoff

Hostnames are not stable identities across container replacement. Future attempt/session IDs should distinguish a new process from an old connection that used the same logical worker name.

## 37. Heartbeats exist, but timeout eviction is deferred

### Problem

Transport closure is not always detected immediately during a network partition. Heartbeats can support faster liveness decisions.

### Current decision

Workers send an empty heartbeat every 10 seconds. The scheduler accepts it, but does not yet persist `last_seen` or run a timeout scanner.

### Why this is incomplete by design

Correct timeout handling must be designed together with job leases and retry attempts. Immediately marking a worker dead can cause duplicate execution if it is merely partitioned and continues running.

### Required follow-up

- Store last-seen monotonic time per worker session.
- Define timeout and grace-period policy.
- Add assignment leases and attempt IDs.
- Make task side effects idempotent.
- Fence updates from expired attempts.

## 38. Immediate jobs only

### Problem

Cron syntax, time zones, missed schedules, repeated runs, and delayed delivery introduce a separate scheduling domain.

### Decision

Every accepted job is eligible for execution immediately.

### Why

This keeps the first version focused on distributed dispatch, worker capacity, persistence, and failure handling.

### Tradeoff

Scheduled and recurring work will require new fields and a process that promotes due executions into the immediate queue.

## 39. Current reliability contract

The implemented system should be understood as follows:

- PostgreSQL makes accepted jobs durable.
- Row locks prevent simultaneous claims of one queued row.
- Conditional updates prevent stale ownership changes.
- Detected worker disconnects requeue unfinished jobs.
- Duplicate task execution is still possible around failures.
- Exactly-once external side effects are not guaranteed.
- An abrupt scheduler crash can leave unfinished jobs without a lease-based recovery mechanism.

The next reliability design should add job attempts and expiring leases before claiming support for multiple active schedulers or strong failure recovery.
