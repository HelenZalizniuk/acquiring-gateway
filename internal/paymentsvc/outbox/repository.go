package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusPublished  = "PUBLISHED"
	StatusFailed     = "FAILED"
)

// Event is an outbox row loaded from PostgreSQL for Kafka publication.
type Event struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	Status        string
	CreatedAt     time.Time
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// ClaimPendingBatch atomically claims a batch of PENDING events (FOR UPDATE SKIP LOCKED)
// and moves them to PROCESSING so concurrent workers do not publish duplicates.
func (r *Repository) ClaimPendingBatch(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = defaultBatchSize
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const claim = `
		WITH picked AS (
			SELECT id
			FROM outbox_events
			WHERE status = $1
			ORDER BY created_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events e
		SET status = $3
		FROM picked
		WHERE e.id = picked.id
		RETURNING e.id, e.aggregate_type, e.aggregate_id, e.event_type, e.payload, e.status, e.created_at
	`

	rows, err := tx.Query(ctx, claim, StatusPending, limit, StatusProcessing)
	if err != nil {
		return nil, fmt.Errorf("claim pending outbox: %w", err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return events, nil
}

// MarkAsProcessed marks successfully published events in a single update.
func (r *Repository) MarkAsProcessed(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}

	const query = `
		UPDATE outbox_events
		SET status = $2, published_at = NOW()
		WHERE id = ANY($1::uuid[])
	`
	if _, err := r.pool.Exec(ctx, query, eventIDs, StatusPublished); err != nil {
		return fmt.Errorf("mark as processed: %w", err)
	}
	return nil
}

// MarkAsFailed marks an event whose publish attempt failed.
func (r *Repository) MarkAsFailed(ctx context.Context, eventID string) error {
	const query = `
		UPDATE outbox_events
		SET status = $2
		WHERE id = $1
	`
	if _, err := r.pool.Exec(ctx, query, eventID, StatusFailed); err != nil {
		return fmt.Errorf("mark as failed: %w", err)
	}
	return nil
}

// MarkPublished marks a single event as published (legacy helper).
func (r *Repository) MarkPublished(ctx context.Context, eventID string) error {
	return r.MarkAsProcessed(ctx, []string{eventID})
}

// Requeue moves an event back to PENDING after a publish failure.
func (r *Repository) Requeue(ctx context.Context, eventID string) error {
	const query = `
		UPDATE outbox_events
		SET status = $2, published_at = NULL
		WHERE id = $1
	`
	if _, err := r.pool.Exec(ctx, query, eventID, StatusPending); err != nil {
		return fmt.Errorf("requeue outbox event: %w", err)
	}
	return nil
}

// RequeueStaleProcessing returns stuck PROCESSING rows to PENDING
// (worker crashed after claim and before publish/ack).
func (r *Repository) RequeueStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error) {
	const query = `
		UPDATE outbox_events
		SET status = $1
		WHERE status = $2
		  AND created_at < NOW() - ($3 * INTERVAL '1 millisecond')
	`
	tag, err := r.pool.Exec(ctx, query, StatusPending, StatusProcessing, olderThan.Milliseconds())
	if err != nil {
		return 0, fmt.Errorf("requeue stale processing: %w", err)
	}
	return tag.RowsAffected(), nil
}

func scanEvents(rows pgx.Rows) ([]Event, error) {
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.ID,
			&e.AggregateType,
			&e.AggregateID,
			&e.EventType,
			&e.Payload,
			&e.Status,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return out, nil
}
