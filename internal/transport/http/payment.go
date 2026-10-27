package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"acquiring-gateway/internal/service"

	"github.com/jackc/pgx/v5/pgconn"
)

type PaymentHandler struct {
	paymentService *service.PaymentService
}

func NewPaymentHandler(paymentService *service.PaymentService) *PaymentHandler {
	return &PaymentHandler{paymentService: paymentService}
}

func (h *PaymentHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/payments", h.createPayment)
}

func (h *PaymentHandler) createPayment(w http.ResponseWriter, r *http.Request) {
	var req service.PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("decode payment request", "err", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
		return
	}

	if err := h.paymentService.ProcessPayment(r.Context(), req); err != nil {
		if isUniqueViolation(err) {
			slog.Error("payment conflict", "err", err, "idempotency_key", req.IdempotencyKey)
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "payment with this idempotency key already exists",
			})
			return
		}

		slog.Error("process payment", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to process payment",
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  service.StatusPending,
		"message": "payment accepted",
	})
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("write json response", "err", err)
	}
}
