package paymenttemporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"acquiring-gateway/internal/paymentsvc/service"

	"go.temporal.io/sdk/temporal"
)

const failNotifyKeyPrefix = "fail-notify-"

// Activities holds Temporal activity dependencies.
type Activities struct {
	Payments *service.PaymentService
	Logger   *slog.Logger
}

func (a *Activities) ChargePaymentActivity(ctx context.Context, input PaymentWorkflowInput) (ChargedPayment, error) {
	a.logger().Info("ChargePaymentActivity",
		"merchant_id", input.MerchantID,
		"idempotency_key", input.IdempotencyKey,
	)

	payment, err := a.Payments.ProcessPayment(ctx, service.ProcessPaymentInput{
		MerchantID:     input.MerchantID,
		Amount:         input.Amount,
		Currency:       input.Currency,
		IdempotencyKey: input.IdempotencyKey,
	})
	if err != nil {
		return ChargedPayment{}, mapActivityError(err)
	}

	return ChargedPayment{
		PaymentID:      payment.ID,
		Status:         payment.Status,
		MerchantID:     payment.MerchantID,
		Amount:         payment.Amount,
		Currency:       payment.Currency,
		IdempotencyKey: payment.IdempotencyKey,
	}, nil
}

// SendNotificationActivity simulates a customer notification.
// Keys prefixed with "fail-notify-" force a failure so compensation can be exercised.
func (a *Activities) SendNotificationActivity(ctx context.Context, payment ChargedPayment) error {
	a.logger().Info("SendNotificationActivity",
		"payment_id", payment.PaymentID,
		"merchant_id", payment.MerchantID,
	)

	if strings.HasPrefix(payment.IdempotencyKey, failNotifyKeyPrefix) {
		return fmt.Errorf("notification provider unavailable for payment %s", payment.PaymentID)
	}

	a.logger().Info("notification sent",
		"payment_id", payment.PaymentID,
		"amount", payment.Amount,
		"currency", payment.Currency,
	)
	return nil
}

func (a *Activities) CompletePaymentActivity(ctx context.Context, payment ChargedPayment) (ChargedPayment, error) {
	updated, err := a.Payments.CompletePayment(ctx, payment.PaymentID)
	if err != nil {
		return ChargedPayment{}, mapActivityError(err)
	}
	return ChargedPayment{
		PaymentID:      updated.ID,
		Status:         updated.Status,
		MerchantID:     updated.MerchantID,
		Amount:         updated.Amount,
		Currency:       updated.Currency,
		IdempotencyKey: updated.IdempotencyKey,
	}, nil
}

// CompensateChargeActivity reverses a charge (refund / cancel) after a later saga step fails.
func (a *Activities) CompensateChargeActivity(ctx context.Context, payment ChargedPayment, reason string) (ChargedPayment, error) {
	a.logger().Warn("CompensateChargeActivity",
		"payment_id", payment.PaymentID,
		"reason", reason,
	)

	updated, err := a.Payments.CompensatePayment(ctx, payment.PaymentID, reason)
	if err != nil {
		return ChargedPayment{}, mapActivityError(err)
	}

	a.logger().Info("payment compensated",
		"payment_id", updated.ID,
		"status", updated.Status,
	)

	return ChargedPayment{
		PaymentID:      updated.ID,
		Status:         updated.Status,
		MerchantID:     updated.MerchantID,
		Amount:         updated.Amount,
		Currency:       updated.Currency,
		IdempotencyKey: updated.IdempotencyKey,
	}, nil
}

func (a *Activities) logger() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return slog.Default()
}

func mapActivityError(err error) error {
	if errors.Is(err, service.ErrInvalidArgument) {
		return temporal.NewNonRetryableApplicationError(err.Error(), nonRetryableErrorType, err)
	}
	return err
}
