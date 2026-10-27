package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"acquiring-gateway/internal/paymentsvc/repository"

	"github.com/google/uuid"
)

const (
	StatusPending   = "PENDING"
	StatusCompleted = "COMPLETED"
	StatusRefunded  = "REFUNDED"
	StatusFailed    = "FAILED"
)

var ErrInvalidArgument = errors.New("invalid argument")

type ProcessPaymentInput struct {
	MerchantID     string
	Amount         string
	Currency       string
	IdempotencyKey string
}

type PaymentRepository interface {
	GetByIdempotencyKey(ctx context.Context, key string) (repository.Payment, error)
	GetByID(ctx context.Context, id string) (repository.Payment, error)
	SaveWithEvent(ctx context.Context, payment repository.Payment, event repository.OutboxEvent) error
	UpdateStatusWithEvent(ctx context.Context, paymentID, newStatus string, event repository.OutboxEvent) (repository.Payment, error)
}

type PaymentService struct {
	repo PaymentRepository
}

func NewPaymentService(repo PaymentRepository) *PaymentService {
	return &PaymentService{repo: repo}
}

// ProcessPayment creates a payment or returns the existing one for the same idempotency key.
func (s *PaymentService) ProcessPayment(ctx context.Context, in ProcessPaymentInput) (repository.Payment, error) {
	if err := validate(in); err != nil {
		return repository.Payment{}, err
	}

	existing, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, repository.ErrNotFound) {
		return repository.Payment{}, fmt.Errorf("lookup payment: %w", err)
	}

	now := time.Now().UTC()
	paymentID := uuid.NewString()

	payment := repository.Payment{
		ID:             paymentID,
		MerchantID:     in.MerchantID,
		Amount:         in.Amount,
		Currency:       strings.ToUpper(in.Currency),
		Status:         StatusPending,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      now,
	}

	event := repository.OutboxEvent{
		ID:            uuid.NewString(),
		AggregateType: repository.AggregateTypePayment,
		AggregateID:   paymentID,
		EventType:     repository.EventTypeCreated,
		Payload:       payment,
		Status:        repository.OutboxStatusPending,
		CreatedAt:     now,
	}

	if err := s.repo.SaveWithEvent(ctx, payment, event); err != nil {
		if repository.IsUniqueViolation(err) {
			// Another request inserted the same key first.
			existing, lookupErr := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey)
			if lookupErr != nil {
				return repository.Payment{}, fmt.Errorf("save payment race recover: %w", lookupErr)
			}
			return existing, nil
		}
		return repository.Payment{}, fmt.Errorf("save payment with outbox event: %w", err)
	}

	return payment, nil
}

// CompletePayment marks the payment completed after a successful notification.
func (s *PaymentService) CompletePayment(ctx context.Context, paymentID string) (repository.Payment, error) {
	return s.repo.UpdateStatusWithEvent(ctx, paymentID, StatusCompleted, repository.OutboxEvent{
		ID:            uuid.NewString(),
		AggregateType: repository.AggregateTypePayment,
		EventType:     repository.EventTypeCompleted,
		Status:        repository.OutboxStatusPending,
		CreatedAt:     time.Now().UTC(),
	})
}

// CompensatePayment refunds or cancels a charge after a later saga step fails.
func (s *PaymentService) CompensatePayment(ctx context.Context, paymentID, reason string) (repository.Payment, error) {
	payment, err := s.repo.UpdateStatusWithEvent(ctx, paymentID, StatusRefunded, repository.OutboxEvent{
		ID:            uuid.NewString(),
		AggregateType: repository.AggregateTypePayment,
		EventType:     repository.EventTypeCompensated,
		Status:        repository.OutboxStatusPending,
		CreatedAt:     time.Now().UTC(),
		Payload: map[string]any{
			"payment_id": paymentID,
			"reason":     reason,
			"status":     StatusRefunded,
		},
	})
	if err != nil {
		return repository.Payment{}, fmt.Errorf("compensate payment: %w", err)
	}
	return payment, nil
}

func validate(in ProcessPaymentInput) error {
	switch {
	case strings.TrimSpace(in.IdempotencyKey) == "":
		return fmt.Errorf("%w: idempotency_key is required", ErrInvalidArgument)
	case strings.TrimSpace(in.MerchantID) == "":
		return fmt.Errorf("%w: merchant_id is required", ErrInvalidArgument)
	case strings.TrimSpace(in.Amount) == "":
		return fmt.Errorf("%w: amount is required", ErrInvalidArgument)
	case strings.TrimSpace(in.Currency) == "":
		return fmt.Errorf("%w: currency is required", ErrInvalidArgument)
	case len(in.Currency) != 3:
		return fmt.Errorf("%w: currency must be ISO-4217 (3 letters)", ErrInvalidArgument)
	default:
		return nil
	}
}
