CREATE TABLE IF NOT EXISTS request_log (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,               -- 'endpoint1' | 'endpoint2'
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- request arrived (~"message in queue")
    slot_acquired_at TIMESTAMPTZ,          -- worker slot acquired (~"celery worker picked it up")
    call_started_at TIMESTAMPTZ,           -- downstream HTTP call started
    call_ended_at TIMESTAMPTZ,             -- downstream HTTP call finished (ok or error)
    status TEXT,                          -- 'success' | 'error'
    error_type TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_request_log_endpoint ON request_log (endpoint);
