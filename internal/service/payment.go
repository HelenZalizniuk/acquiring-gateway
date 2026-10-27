package service

import (
	"context"
	"fmt"
	"time"

	"acquiring-gateway/internal/repository"

	"github.com/google/uuid"
)

const StatusPending = "PENDING"

type PaymentRequest struct {
	MerchantID     string `json:"merchant_id"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"idempotency_key"`
}

type PaymentRepository interface {
	SaveWithEvent(ctx context.Context, payment repository.Payment, event repository.OutboxEvent) error
}

type PaymentService struct {
	repo PaymentRepository
}

func NewPaymentService(repo PaymentRepository) *PaymentService {
	return &PaymentService{repo: repo}
}

func (s *PaymentService) ProcessPayment(ctx context.Context, req PaymentRequest) error {
	now := time.Now().UTC()
	paymentID := uuid.NewString()

	payment := repository.Payment{
		ID:             paymentID,
		MerchantID:     req.MerchantID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Status:         StatusPending,
		IdempotencyKey: req.IdempotencyKey,
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
		return fmt.Errorf("save payment with outbox event: %w", err)
	}

	return nil
}
