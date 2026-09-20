CREATE INDEX jobs_queued_dispatch_idx
    ON jobs (priority DESC, created_at ASC)
    WHERE status = 'queued';
