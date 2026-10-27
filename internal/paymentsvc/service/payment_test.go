package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"acquiring-gateway/internal/paymentsvc/repository"

	"github.com/jackc/pgx/v5/pgconn"
)

type mockPaymentRepo struct {
	byKey     map[string]repository.Payment
	saveErr   error
	saved     []repository.Payment
	lookupErr error
}

func (m *mockPaymentRepo) GetByIdempotencyKey(_ context.Context, key string) (repository.Payment, error) {
	if m.lookupErr != nil {
		return repository.Payment{}, m.lookupErr
	}
	p, ok := m.byKey[key]
	if !ok {
		return repository.Payment{}, repository.ErrNotFound
	}
	return p, nil
}

func (m *mockPaymentRepo) GetByID(context.Context, string) (repository.Payment, error) {
	return repository.Payment{}, repository.ErrNotFound
}

func (m *mockPaymentRepo) SaveWithEvent(_ context.Context, payment repository.Payment, _ repository.OutboxEvent) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	if m.byKey == nil {
		m.byKey = map[string]repository.Payment{}
	}
	m.byKey[payment.IdempotencyKey] = payment
	m.saved = append(m.saved, payment)
	return nil
}

func (m *mockPaymentRepo) UpdateStatusWithEvent(context.Context, string, string, repository.OutboxEvent) (repository.Payment, error) {
	return repository.Payment{}, nil
}

func TestProcessPayment_Validation(t *testing.T) {
	svc := NewPaymentService(&mockPaymentRepo{})
	ctx := context.Background()

	tests := []struct {
		name string
		in   ProcessPaymentInput
		want string
	}{
		{
			name: "missing amount",
			in: ProcessPaymentInput{
				MerchantID: "m-1", Amount: "", Currency: "USD", IdempotencyKey: "k-1",
			},
			want: "amount is required",
		},
		{
			name: "missing currency",
			in: ProcessPaymentInput{
				MerchantID: "m-1", Amount: "10.00", Currency: "", IdempotencyKey: "k-1",
			},
			want: "currency is required",
		},
		{
			name: "currency not ISO-4217",
			in: ProcessPaymentInput{
				MerchantID: "m-1", Amount: "10.00", Currency: "US", IdempotencyKey: "k-1",
			},
			want: "currency must be ISO-4217 (3 letters)",
		},
		{
			name: "missing merchant",
			in: ProcessPaymentInput{
				MerchantID: "", Amount: "10.00", Currency: "USD", IdempotencyKey: "k-1",
			},
			want: "merchant_id is required",
		},
		{
			name: "missing idempotency key",
			in: ProcessPaymentInput{
				MerchantID: "m-1", Amount: "10.00", Currency: "USD", IdempotencyKey: "",
			},
			want: "idempotency_key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.ProcessPayment(ctx, tt.in)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestProcessPayment_CreatesPendingPayment(t *testing.T) {
	repo := &mockPaymentRepo{}
	svc := NewPaymentService(repo)

	got, err := svc.ProcessPayment(context.Background(), ProcessPaymentInput{
		MerchantID:     "merchant-1",
		Amount:         "100.00",
		Currency:       "usd",
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if got.ID == "" {
		t.Fatal("expected generated payment id")
	}
	if got.Status != StatusPending {
		t.Errorf("status = %q, want PENDING", got.Status)
	}
	if got.Currency != "USD" {
		t.Errorf("currency = %q, want USD", got.Currency)
	}
	if got.Amount != "100.00" {
		t.Errorf("amount = %q, want 100.00", got.Amount)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("saved %d payments, want 1", len(repo.saved))
	}
}

func TestProcessPayment_ReturnsExistingOnIdempotentReplay(t *testing.T) {
	existing := repository.Payment{
		ID:             "pay-existing",
		MerchantID:     "merchant-1",
		Amount:         "50.00",
		Currency:       "EUR",
		Status:         StatusPending,
		IdempotencyKey: "idem-dup",
	}
	repo := &mockPaymentRepo{byKey: map[string]repository.Payment{"idem-dup": existing}}
	svc := NewPaymentService(repo)

	got, err := svc.ProcessPayment(context.Background(), ProcessPaymentInput{
		MerchantID:     "merchant-1",
		Amount:         "50.00",
		Currency:       "EUR",
		IdempotencyKey: "idem-dup",
	})
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if got.ID != existing.ID {
		t.Errorf("id = %q, want %q", got.ID, existing.ID)
	}
	if len(repo.saved) != 0 {
		t.Errorf("SaveWithEvent called %d times, want 0", len(repo.saved))
	}
}

func TestProcessPayment_RecoversUniqueViolationRace(t *testing.T) {
	existing := repository.Payment{
		ID:             "pay-won",
		IdempotencyKey: "idem-race",
		Status:         StatusPending,
		Currency:       "USD",
		Amount:         "1.00",
		MerchantID:     "m-1",
	}
	repo := &mockPaymentRepo{
		saveErr: &pgconn.PgError{Code: "23505"},
	}
	lookups := 0
	wrapping := &lookupThenFoundRepo{
		inner:    repo,
		existing: existing,
		lookups:  &lookups,
	}

	got, err := NewPaymentService(wrapping).ProcessPayment(context.Background(), ProcessPaymentInput{
		MerchantID:     "m-1",
		Amount:         "1.00",
		Currency:       "USD",
		IdempotencyKey: "idem-race",
	})
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if got.ID != existing.ID {
		t.Errorf("id = %q, want %q", got.ID, existing.ID)
	}
}

// lookupThenFoundRepo returns ErrNotFound once, then the existing payment.
type lookupThenFoundRepo struct {
	inner    *mockPaymentRepo
	existing repository.Payment
	lookups  *int
}

func (r *lookupThenFoundRepo) GetByIdempotencyKey(ctx context.Context, key string) (repository.Payment, error) {
	*r.lookups++
	if *r.lookups == 1 {
		return repository.Payment{}, repository.ErrNotFound
	}
	return r.existing, nil
}

func (r *lookupThenFoundRepo) GetByID(ctx context.Context, id string) (repository.Payment, error) {
	return r.inner.GetByID(ctx, id)
}

func (r *lookupThenFoundRepo) SaveWithEvent(ctx context.Context, payment repository.Payment, event repository.OutboxEvent) error {
	return r.inner.SaveWithEvent(ctx, payment, event)
}

func (r *lookupThenFoundRepo) UpdateStatusWithEvent(ctx context.Context, paymentID, newStatus string, event repository.OutboxEvent) (repository.Payment, error) {
	return r.inner.UpdateStatusWithEvent(ctx, paymentID, newStatus, event)
}

