package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/domain"
	"github.com/muthuku37/distributed-scheduler/internal/task"
)

const migrationLockID int64 = 734203159

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create schema migrations table: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		applied, err := migrationApplied(ctx, tx, entry.Name())
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		migration, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}

		if _, err := tx.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}

		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)",
			entry.Name(),
		); err != nil {
			return fmt.Errorf("record migration %s: %w", entry.Name(), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}

	return nil
}

func migrationApplied(ctx context.Context, tx pgx.Tx, version string) (bool, error) {
	var applied bool
	err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)",
		version,
	).Scan(&applied)
	if err != nil {
		return false, fmt.Errorf("check migration %s: %w", version, err)
	}

	return applied, nil
}

func (s *Store) CreateJob(ctx context.Context, job domain.Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("encode payload for job %s: %w", job.ID, err)
	}

	const query = `
		INSERT INTO jobs (id, task, payload, priority, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err = s.pool.Exec(ctx, query,
		string(job.ID),
		string(job.Task),
		payload,
		job.Priority,
		string(job.Status),
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job %s: %w", job.ID, err)
	}

	return nil
}

// ClaimNextQueuedJob atomically assigns the highest-priority compatible job.
func (s *Store) ClaimNextQueuedJob(
	ctx context.Context,
	workerID string,
	supportedTasks []task.Name,
) (domain.Job, bool, error) {
	tasks := make([]string, 0, len(supportedTasks))
	for _, supportedTask := range supportedTasks {
		tasks = append(tasks, string(supportedTask))
	}
	if len(tasks) == 0 {
		return domain.Job{}, false, nil
	}

	const query = `
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
		RETURNING id, task, payload, priority, status, assigned_worker_id,
		          COALESCE(result, ''), COALESCE(error_message, ''), created_at, updated_at
	`

	var (
		job         domain.Job
		taskValue   string
		statusValue string
		payload     []byte
	)
	err := s.pool.QueryRow(ctx, query, tasks, workerID).Scan(
		&job.ID,
		&taskValue,
		&payload,
		&job.Priority,
		&statusValue,
		&job.AssignedWorkerID,
		&job.Result,
		&job.ErrorMessage,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, nil
	}
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("claim queued job: %w", err)
	}

	job.Task = task.Name(taskValue)
	job.Status = domain.JobStatus(statusValue)
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return domain.Job{}, false, fmt.Errorf("decode payload for job %s: %w", job.ID, err)
	}
	return job, true, nil
}

// RequeueJob releases one assignment that could not be delivered to its worker.
func (s *Store) RequeueJob(ctx context.Context, jobID domain.JobID, workerID string) error {
	const query = `
		UPDATE jobs
		SET status = 'queued', assigned_worker_id = NULL, updated_at = NOW()
		WHERE id = $1 AND assigned_worker_id = $2 AND status = 'assigned'
	`

	if _, err := s.pool.Exec(ctx, query, string(jobID), workerID); err != nil {
		return fmt.Errorf("requeue job %s: %w", jobID, err)
	}
	return nil
}

// RequeueWorkerJobs releases unfinished jobs after a worker disconnects.
func (s *Store) RequeueWorkerJobs(ctx context.Context, workerID string) error {
	const query = `
		UPDATE jobs
		SET status = 'queued', assigned_worker_id = NULL, updated_at = NOW()
		WHERE assigned_worker_id = $1 AND status IN ('assigned', 'running')
	`

	if _, err := s.pool.Exec(ctx, query, workerID); err != nil {
		return fmt.Errorf("requeue jobs for worker %s: %w", workerID, err)
	}
	return nil
}

// UpdateJobStatus persists progress or a terminal result from the assigned worker.
func (s *Store) UpdateJobStatus(ctx context.Context, workerID string, update domain.JobUpdate) error {
	const query = `
		UPDATE jobs
		SET status = $3,
		    result = NULLIF($4, ''),
		    error_message = NULLIF($5, ''),
		    updated_at = NOW()
		WHERE id = $1
		  AND assigned_worker_id = $2
		  AND (
		      ($3 = 'running' AND status = 'assigned')
		      OR ($3 IN ('succeeded', 'failed') AND status IN ('assigned', 'running'))
		  )
	`

	result, err := s.pool.Exec(ctx, query,
		string(update.JobID),
		workerID,
		string(update.Status),
		update.Result,
		update.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("update job %s: %w", update.JobID, err)
	}
	if result.RowsAffected() == 0 {
		return errors.New("job update rejected because its assignment or state changed")
	}
	return nil
}
