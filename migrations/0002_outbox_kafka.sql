-- Outbox statuses: PENDING → PROCESSING → PUBLISHED (or back to PENDING on failure)
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (created_at)
    WHERE status = 'PENDING';

-- Consumer idempotency: deduplicate by event_id (at-least-once)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
