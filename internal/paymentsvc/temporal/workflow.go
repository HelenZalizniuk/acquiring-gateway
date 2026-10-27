package paymenttemporal

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

const PaymentQueue = "payment-saga"

type PaymentWorkflowInput struct {
	MerchantID     string `json:"merchant_id"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"idempotency_key"`
}

type PaymentWorkflowResult struct {
	PaymentID      string `json:"payment_id"`
	Status         string `json:"status"`
	MerchantID     string `json:"merchant_id"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"idempotency_key"`
	Compensated    bool   `json:"compensated,omitempty"`
}

type ChargedPayment struct {
	PaymentID      string `json:"payment_id"`
	Status         string `json:"status"`
	MerchantID     string `json:"merchant_id"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"idempotency_key"`
}

// ProcessPaymentWorkflow runs Charge → Notify → Complete and compensates if Notify or Complete fails.
func ProcessPaymentWorkflow(ctx workflow.Context, input PaymentWorkflowInput) (PaymentWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ProcessPaymentWorkflow started",
		"merchant_id", input.MerchantID,
		"idempotency_key", input.IdempotencyKey,
	)

	policies := DefaultActivityPolicies()
	var activities *Activities

	chargeCtx := workflow.WithActivityOptions(ctx, policies.Charge)
	var charged ChargedPayment
	if err := workflow.ExecuteActivity(chargeCtx, activities.ChargePaymentActivity, input).Get(ctx, &charged); err != nil {
		logger.Error("ChargePaymentActivity failed", "error", err)
		return PaymentWorkflowResult{}, fmt.Errorf("charge payment: %w", err)
	}

	notifyCtx := workflow.WithActivityOptions(ctx, policies.Notify)
	if err := workflow.ExecuteActivity(notifyCtx, activities.SendNotificationActivity, charged).Get(ctx, nil); err != nil {
		logger.Error("SendNotificationActivity failed, starting compensation", "error", err)
		return compensate(ctx, policies, activities, charged, err)
	}

	completeCtx := workflow.WithActivityOptions(ctx, policies.Complete)
	var completed ChargedPayment
	if err := workflow.ExecuteActivity(completeCtx, activities.CompletePaymentActivity, charged).Get(ctx, &completed); err != nil {
		logger.Error("CompletePaymentActivity failed, starting compensation", "error", err)
		return compensate(ctx, policies, activities, charged, err)
	}

	logger.Info("ProcessPaymentWorkflow completed", "payment_id", completed.PaymentID, "status", completed.Status)
	return PaymentWorkflowResult{
		PaymentID:      completed.PaymentID,
		Status:         completed.Status,
		MerchantID:     completed.MerchantID,
		Amount:         completed.Amount,
		Currency:       completed.Currency,
		IdempotencyKey: completed.IdempotencyKey,
	}, nil
}

func compensate(
	ctx workflow.Context,
	policies ActivityPolicies,
	activities *Activities,
	charged ChargedPayment,
	cause error,
) (PaymentWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	compCtx := workflow.WithActivityOptions(ctx, policies.Compensate)

	var compensated ChargedPayment
	compErr := workflow.ExecuteActivity(
		compCtx,
		activities.CompensateChargeActivity,
		charged,
		cause.Error(),
	).Get(ctx, &compensated)
	if compErr != nil {
		logger.Error("CompensateChargeActivity failed", "error", compErr)
		return PaymentWorkflowResult{}, fmt.Errorf("step failed (%v) and compensate failed: %w", cause, compErr)
	}

	logger.Warn("payment compensated",
		"payment_id", compensated.PaymentID,
		"status", compensated.Status,
		"cause", cause.Error(),
	)
	return PaymentWorkflowResult{
		PaymentID:      compensated.PaymentID,
		Status:         compensated.Status,
		MerchantID:     compensated.MerchantID,
		Amount:         compensated.Amount,
		Currency:       compensated.Currency,
		IdempotencyKey: compensated.IdempotencyKey,
		Compensated:    true,
	}, nil
}