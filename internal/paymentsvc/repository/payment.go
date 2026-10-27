package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OutboxStatusPending  = "PENDING"
	AggregateTypePayment = "payment"
	EventTypeCreated     = "PaymentCreated"
	EventTypeCompleted   = "PaymentCompleted"
	EventTypeCompensated = "PaymentCompensated"
)

var ErrNotFound = errors.New("payment not found")

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

func (r *PaymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (Payment, error) {
	const query = `
		SELECT id, merchant_id, amount::text, currency, status, idempotency_key, created_at
		FROM payments
		WHERE idempotency_key = $1
	`

	var p Payment
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&p.ID,
		&p.MerchantID,
		&p.Amount,
		&p.Currency,
		&p.Status,
		&p.IdempotencyKey,
		&p.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment by idempotency key: %w", err)
	}
	return p, nil
}

func (r *PaymentRepository) GetByID(ctx context.Context, id string) (Payment, error) {
	const query = `
		SELECT id, merchant_id, amount::text, currency, status, idempotency_key, created_at
		FROM payments
		WHERE id = $1
	`

	var p Payment
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&p.ID,
		&p.MerchantID,
		&p.Amount,
		&p.Currency,
		&p.Status,
		&p.IdempotencyKey,
		&p.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment by id: %w", err)
	}
	return p, nil
}

// UpdateStatusWithEvent updates payment status and writes an outbox event in one transaction.
func (r *PaymentRepository) UpdateStatusWithEvent(ctx context.Context, paymentID, newStatus string, event OutboxEvent) (Payment, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Payment{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const updateQuery = `
		UPDATE payments
		SET status = $2
		WHERE id = $1
		RETURNING id, merchant_id, amount::text, currency, status, idempotency_key, created_at
	`

	var p Payment
	err = tx.QueryRow(ctx, updateQuery, paymentID, newStatus).Scan(
		&p.ID,
		&p.MerchantID,
		&p.Amount,
		&p.Currency,
		&p.Status,
		&p.IdempotencyKey,
		&p.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("update payment status: %w", err)
	}

	event.AggregateID = p.ID
	if event.Payload == nil {
		event.Payload = p
	}
	if err := insertOutboxEvent(ctx, tx, event); err != nil {
		return Payment{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Payment{}, fmt.Errorf("commit tx: %w", err)
	}
	return p, nil
}

// SaveWithEvent writes the payment and outbox event in a single pgx.Tx.
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

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
