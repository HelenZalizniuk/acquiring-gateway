package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OutboxStatusPending  = "PENDING"
	AggregateTypePayment = "payment"
	EventTypeCreated     = "PaymentCreated"
)

type Payment struct {
	ID             string    `json:"id"`
	MerchantID     string    `json:"merchant_id"`
	Amount         string    `json:"amount"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

type OutboxEvent struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       any
	Status        string
	CreatedAt     time.Time
}

type PaymentRepository struct {
	pool *pgxpool.Pool
}

func NewPaymentRepository(pool *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{pool: pool}
}

// SaveWithEvent writes the payment and outbox event in a single pgx.Tx.
// Any INSERT error rolls back both operations.
func (r *PaymentRepository) SaveWithEvent(ctx context.Context, payment Payment, event OutboxEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertPayment(ctx, tx, payment); err != nil {
		return err
	}

	if err := insertOutboxEvent(ctx, tx, event); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func insertPayment(ctx context.Context, tx pgx.Tx, payment Payment) error {
	createdAt := payment.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO payments (
			id, merchant_id, amount, currency, status, idempotency_key, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	if _, err := tx.Exec(ctx, query,
		payment.ID,
		payment.MerchantID,
		payment.Amount,
		payment.Currency,
		payment.Status,
		payment.IdempotencyKey,
		createdAt,
	); err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	return nil
}

func insertOutboxEvent(ctx context.Context, tx pgx.Tx, event OutboxEvent) error {
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	status := event.Status
	if status == "" {
		status = OutboxStatusPending
	}

	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	const query = `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, status, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	if _, err := tx.Exec(ctx, query,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		payload,
		status,
		createdAt,
	); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	return nil
}
