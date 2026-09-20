CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    task TEXT NOT NULL CHECK (BTRIM(task) <> ''),
    priority INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'assigned', 'running', 'succeeded', 'failed')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
