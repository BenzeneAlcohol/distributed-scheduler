ALTER TABLE jobs
    ADD COLUMN payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN assigned_worker_id TEXT,
    ADD COLUMN result TEXT,
    ADD COLUMN error_message TEXT;

CREATE INDEX jobs_assigned_worker_idx
    ON jobs (assigned_worker_id)
    WHERE status IN ('assigned', 'running');
