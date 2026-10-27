package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	paymentv1 "acquiring-gateway/gen/go/payment/v1"
	"acquiring-gateway/internal/paymentsvc/repository"
	"acquiring-gateway/internal/paymentsvc/service"
	paymenttemporal "acquiring-gateway/internal/paymentsvc/temporal"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PaymentServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	svc            *service.PaymentService
	temporalClient client.Client
	queue          string
	logger         *slog.Logger
}

func NewPaymentServer(svc *service.PaymentService, logger *slog.Logger) *PaymentServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &PaymentServer{svc: svc, logger: logger}
}

func (s *PaymentServer) WithTemporal(c client.Client, queue string) *PaymentServer {
	s.temporalClient = c
	s.queue = queue
	return s
}

func (s *PaymentServer) ProcessPayment(
	ctx context.Context,
	req *paymentv1.ProcessPaymentRequest,
) (*paymentv1.ProcessPaymentResponse, error) {
	s.logger.Info("ProcessPayment request",
		"merchant_id", req.GetMerchantId(),
		"idempotency_key", req.GetIdempotencyKey(),
		"currency", req.GetCurrency(),
	)

	if s.temporalClient != nil {
		return s.processViaTemporal(ctx, req)
	}
	return s.processDirect(ctx, req)
}

func (s *PaymentServer) processViaTemporal(
	ctx context.Context,
	req *paymentv1.ProcessPaymentRequest,
) (*paymentv1.ProcessPaymentResponse, error) {
	result, err := paymenttemporal.StartProcessPayment(ctx, s.temporalClient, s.queue, paymenttemporal.PaymentWorkflowInput{
		MerchantID:     req.GetMerchantId(),
		Amount:         req.GetAmount(),
		Currency:       req.GetCurrency(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		s.logger.Error("temporal workflow failed", "err", err)
		return nil, s.mapError(err)
	}

	if result.Compensated {
		s.logger.Warn("payment compensated by saga",
			"payment_id", result.PaymentID,
			"status", result.Status,
		)
		return nil, status.Errorf(codes.Aborted,
			"payment compensated after notification failure: payment_id=%s status=%s",
			result.PaymentID, result.Status,
		)
	}

	s.logger.Info("ProcessPayment success via temporal",
		"payment_id", result.PaymentID,
		"status", result.Status,
	)

	return &paymentv1.ProcessPaymentResponse{
		PaymentId:      result.PaymentID,
		Status:         result.Status,
		MerchantId:     result.MerchantID,
		Amount:         result.Amount,
		Currency:       result.Currency,
		IdempotencyKey: result.IdempotencyKey,
	}, nil
}

func (s *PaymentServer) processDirect(
	ctx context.Context,
	req *paymentv1.ProcessPaymentRequest,
) (*paymentv1.ProcessPaymentResponse, error) {
	payment, err := s.svc.ProcessPayment(ctx, service.ProcessPaymentInput{
		MerchantID:     req.GetMerchantId(),
		Amount:         req.GetAmount(),
		Currency:       req.GetCurrency(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, s.mapError(err)
	}

	s.logger.Info("ProcessPayment success",
		"payment_id", payment.ID,
		"status", payment.Status,
		"idempotency_key", payment.IdempotencyKey,
	)

	return toProto(payment), nil
}

func toProto(p repository.Payment) *paymentv1.ProcessPaymentResponse {
	return &paymentv1.ProcessPaymentResponse{
		PaymentId:      p.ID,
		Status:         p.Status,
		MerchantId:     p.MerchantID,
		Amount:         p.Amount,
		Currency:       p.Currency,
		IdempotencyKey: p.IdempotencyKey,
	}
}

func (s *PaymentServer) mapError(err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		s.logger.Error("ProcessPayment failed", "err", err)
		return status.Errorf(codes.Internal, "failed to process payment")
	}
}
