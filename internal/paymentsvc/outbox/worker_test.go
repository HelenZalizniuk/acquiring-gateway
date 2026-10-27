package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"
)

type mockRepository struct {
	mu        sync.Mutex
	processed []string
	failed    []string
}

func (m *mockRepository) ClaimPendingBatch(context.Context, int) ([]Event, error) {
	return nil, nil
}

func (m *mockRepository) RequeueStaleProcessing(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockRepository) MarkAsProcessed(_ context.Context, eventIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed = append(m.processed, eventIDs...)
	return nil
}

func (m *mockRepository) MarkAsFailed(_ context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = append(m.failed, eventID)
	return nil
}

func (m *mockRepository) snapshot() (processed, failed []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	processed = append([]string(nil), m.processed...)
	failed = append([]string(nil), m.failed...)
	sort.Strings(processed)
	sort.Strings(failed)
	return processed, failed
}

type mockPublisher struct {
	mu        sync.Mutex
	failIDs   map[string]error
	published []string
	started   chan struct{}
	block     <-chan struct{}
}

func (m *mockPublisher) Publish(ctx context.Context, event Event) error {
	if m.started != nil {
		select {
		case <-m.started:
		default:
			close(m.started)
		}
	}
	if m.block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.block:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.failIDs[event.ID]; ok {
		return err
	}
	m.published = append(m.published, event.ID)
	return nil
}

func (m *mockPublisher) publishedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]string(nil), m.published...)
	sort.Strings(out)
	return out
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestProcessBatch(t *testing.T) {
	events := []Event{
		{ID: "evt-1", AggregateID: "pay-1", EventType: "PaymentCreated"},
		{ID: "evt-2", AggregateID: "pay-2", EventType: "PaymentCreated"},
		{ID: "evt-3", AggregateID: "pay-3", EventType: "PaymentCreated"},
	}
	publishErr := errors.New("broker unavailable")

	tests := []struct {
		name           string
		failIDs        map[string]error
		cancel         bool
		wantPublished  []string
		wantProcessed  []string
		wantFailed     []string
		wantErr        error
	}{
		{
			name:          "successful batch processing",
			wantPublished: []string{"evt-1", "evt-2", "evt-3"},
			wantProcessed: []string{"evt-1", "evt-2", "evt-3"},
		},
		{
			name:          "partial failure marks failed events",
			failIDs:       map[string]error{"evt-2": publishErr},
			wantPublished: []string{"evt-1", "evt-3"},
			wantProcessed: []string{"evt-1", "evt-3"},
			wantFailed:    []string{"evt-2"},
		},
		{
			name:    "context cancellation stops processing",
			cancel:  true,
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mockRepository{}
			pub := &mockPublisher{failIDs: tt.failIDs}
			w := NewWorker(repo, pub, Config{WorkersCount: 2}, silentLogger())

			ctx := context.Background()
			if tt.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := w.processBatch(ctx, events)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("processBatch error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("processBatch: %v", err)
			}

			if got := pub.publishedIDs(); !equalStrings(got, tt.wantPublished) {
				t.Errorf("published = %v, want %v", got, tt.wantPublished)
			}
			processed, failed := repo.snapshot()
			if !equalStrings(processed, tt.wantProcessed) {
				t.Errorf("processed = %v, want %v", processed, tt.wantProcessed)
			}
			if !equalStrings(failed, tt.wantFailed) {
				t.Errorf("failed = %v, want %v", failed, tt.wantFailed)
			}
		})
	}
}

func TestProcessBatch_CancelAfterPublishStarted(t *testing.T) {
	events := []Event{
		{ID: "evt-a", AggregateID: "pay-a"},
		{ID: "evt-b", AggregateID: "pay-b"},
	}

	started := make(chan struct{})
	release := make(chan struct{})
	repo := &mockRepository{}
	pub := &mockPublisher{started: started, block: release}
	w := NewWorker(repo, pub, Config{WorkersCount: 1}, silentLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- w.processBatch(ctx, events)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Publish to start")
	}
	cancel()
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("processBatch error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for processBatch to return")
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
