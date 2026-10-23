-- CREATE TABLE outbox_events (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     event_type TEXT NOT NULL,
--     aggregate_type TEXT NOT NULL,
--     aggregate_id UUID NOT NULL,
--     payload JSONB NOT NULL,
--     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
--     published_at TIMESTAMPTZ
-- );
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,

    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,

    locked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- SELECT *
-- FROM outbox_events
-- WHERE published_at IS NULL
-- ORDER BY created_at
-- LIMIT 100;