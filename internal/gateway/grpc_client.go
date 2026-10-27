package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	paymentv1 "acquiring-gateway/gen/go/payment/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PaymentGRPCClient maps HTTP /v1/payments to gRPC PaymentService.ProcessPayment.
type PaymentGRPCClient struct {
	client      paymentv1.PaymentServiceClient
	conn        *grpc.ClientConn
	callTimeout time.Duration
	logger      *slog.Logger
}

func NewPaymentGRPCClient(addr string, logger *slog.Logger) (*PaymentGRPCClient, error) {
	return NewPaymentGRPCClientWithConfig(addr, DefaultGRPCClientConfig(), logger)
}

func NewPaymentGRPCClientWithConfig(addr string, cfg GRPCClientConfig, logger *slog.Logger) (*PaymentGRPCClient, error) {
	cfg = cfg.withDefaults()

	target, err := grpcDialTarget(addr)
	if err != nil {
		return nil, err
	}

	conn, err := dialPaymentGRPC(target, cfg)
	if err != nil {
		return nil, fmt.Errorf("dial payment grpc %q: %w", target, err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &PaymentGRPCClient{
		client:      paymentv1.NewPaymentServiceClient(conn),
		conn:        conn,
		callTimeout: cfg.CallTimeout,
		logger:      logger,
	}, nil
}

func (c *PaymentGRPCClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *PaymentGRPCClient) HandleProcessPayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	req, err := decodePaymentRequest(r)
	if err != nil {
		c.logger.Error("decode payment request", "err", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
		return
	}

	timeout := c.callTimeout
	if timeout <= 0 {
		timeout = DefaultGRPCClientConfig().CallTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	resp, err := c.client.ProcessPayment(ctx, req)
	if err != nil {
		httpStatus, msg := mapGRPCError(err)
		c.logger.Error("grpc ProcessPayment failed",
			"err", err,
			"http_status", httpStatus,
			"merchant_id", req.GetMerchantId(),
			"idempotency_key", req.GetIdempotencyKey(),
		)
		writeJSON(w, httpStatus, map[string]string{"error": msg})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"payment_id":      resp.GetPaymentId(),
		"status":          resp.GetStatus(),
		"merchant_id":     resp.GetMerchantId(),
		"amount":          resp.GetAmount(),
		"currency":        resp.GetCurrency(),
		"idempotency_key": resp.GetIdempotencyKey(),
	})
}

type httpPaymentBody struct {
	IdempotencyKey string `json:"idempotency_key"`
	MerchantID     string `json:"merchant_id"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
}

func decodePaymentRequest(r *http.Request) (*paymentv1.ProcessPaymentRequest, error) {
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var payload httpPaymentBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("unmarshal json: %w", err)
		}
	}

	merchantID := firstNonEmpty(payload.MerchantID, r.Header.Get(HeaderMerchantID))
	idempotencyKey := firstNonEmpty(payload.IdempotencyKey, r.Header.Get("Idempotency-Key"))

	req := &paymentv1.ProcessPaymentRequest{
		IdempotencyKey: idempotencyKey,
		MerchantId:     merchantID,
		Amount:         payload.Amount,
		Currency:       payload.Currency,
	}

	if req.MerchantId == "" || req.Amount == "" || req.Currency == "" || req.IdempotencyKey == "" {
		return nil, fmt.Errorf("missing required fields")
	}

	return req, nil
}

func mapGRPCError(err error) (int, string) {
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusBadGateway, "upstream unavailable"
	}

	msg := st.Message()
	if msg == "" {
		msg = st.Code().String()
	}

	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, msg
	case codes.NotFound:
		return http.StatusNotFound, msg
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict, msg
	case codes.PermissionDenied:
		return http.StatusForbidden, msg
	case codes.Unauthenticated:
		return http.StatusUnauthorized, msg
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, msg
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed, msg
	case codes.Unimplemented:
		return http.StatusNotImplemented, msg
	case codes.Unavailable:
		return http.StatusServiceUnavailable, msg
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, msg
	case codes.Canceled:
		return 499, msg
	default:
		return http.StatusInternalServerError, msg
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// grpcDialTarget accepts host:port or a URL such as http://host:port and returns host:port.
func grpcDialTarget(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty grpc address")
	}

	raw = strings.TrimPrefix(raw, "https://")
	raw = strings.TrimPrefix(raw, "http://")
	raw = strings.TrimPrefix(raw, "grpc://")
	raw = strings.TrimSuffix(raw, "/")

	if raw == "" {
		return "", fmt.Errorf("invalid grpc address")
	}
	return raw, nil
}
