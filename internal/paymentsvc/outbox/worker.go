package outbox

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultWorkersCount   = 8
	defaultBatchSize      = 50
	defaultPollInterval   = 500 * time.Millisecond
	defaultStaleOlderThan = 5 * time.Minute
	shutdownMarkTimeout   = 3 * time.Second
)

type Config struct {
	WorkersCount   int
	BatchSize      int
	PollInterval   time.Duration
	StaleOlderThan time.Duration
}

func DefaultConfig() Config {
	return Config{
		WorkersCount:   defaultWorkersCount,
		BatchSize:      defaultBatchSize,
		PollInterval:   defaultPollInterval,
		StaleOlderThan: defaultStaleOlderThan,
	}
}

// DefaultWorkerConfig is an alias of DefaultConfig for existing call sites.
func DefaultWorkerConfig() Config {
	return DefaultConfig()
}

type WorkerConfig = Config

// EventRepository is the persistence used by Worker.
type EventRepository interface {
	ClaimPendingBatch(ctx context.Context, limit int) ([]Event, error)
	MarkAsProcessed(ctx context.Context, eventIDs []string) error
	MarkAsFailed(ctx context.Context, eventID string) error
	RequeueStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error)
}

// EventPublisher publishes a claimed outbox event to the broker.
type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
}

// Worker polls PENDING outbox events in batches and publishes them to Kafka
// using a parallel worker pool. MarkAsProcessed runs only after broker ack.
type Worker struct {
	repo      EventRepository
	publisher EventPublisher
	cfg       Config
	logger    *slog.Logger
}

func NewWorker(repo EventRepository, publisher EventPublisher, cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	defaults := DefaultConfig()
	if cfg.WorkersCount <= 0 {
		cfg.WorkersCount = defaults.WorkersCount
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaults.BatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	if cfg.StaleOlderThan <= 0 {
		cfg.StaleOlderThan = defaults.StaleOlderThan
	}
	return &Worker{repo: repo, publisher: publisher, cfg: cfg, logger: logger}
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("outbox worker started",
		"workers_count", w.cfg.WorkersCount,
		"batch_size", w.cfg.BatchSize,
		"poll_interval", w.cfg.PollInterval.String(),
	)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	staleTicker := time.NewTicker(w.cfg.StaleOlderThan / 2)
	defer staleTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("outbox worker stopping")
			return ctx.Err()
		case <-staleTicker.C:
			n, err := w.repo.RequeueStaleProcessing(ctx, w.cfg.StaleOlderThan)
			if err != nil {
				w.logger.Error("requeue stale processing failed", "err", err)
			} else if n > 0 {
				w.logger.Warn("requeued stale processing events", "count", n)
			}
		case <-ticker.C:
			if err := w.pollOnce(ctx); err != nil && ctx.Err() == nil {
				w.logger.Error("outbox poll failed", "err", err)
			}
		}
	}
}

func (w *Worker) pollOnce(ctx context.Context) error {
	events, err := w.repo.ClaimPendingBatch(ctx, w.cfg.BatchSize)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	w.logger.Info("claimed outbox batch", "count", len(events))
	return w.processBatch(ctx, events)
}

func (w *Worker) processBatch(ctx context.Context, events []Event) error {
	jobs := make(chan Event)
	processedIDs := make(chan string)

	var wg sync.WaitGroup
	workerCount := w.cfg.WorkersCount
	if workerCount > len(events) {
		workerCount = len(events)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runWorker(ctx, jobs, processedIDs)
		}()
	}

	go func() {
		defer close(jobs)
		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case jobs <- event:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(processedIDs)
	}()

	successIDs := make([]string, 0, len(events))
	for id := range processedIDs {
		successIDs = append(successIDs, id)
	}

	if err := ctx.Err(); err != nil {
		markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownMarkTimeout)
		defer cancel()
		if markErr := w.repo.MarkAsProcessed(markCtx, successIDs); markErr != nil {
			w.logger.Error("mark as processed failed after cancel", "err", markErr, "count", len(successIDs))
		}
		return err
	}

	if err := w.repo.MarkAsProcessed(ctx, successIDs); err != nil {
		return err
	}

	w.logger.Info("outbox batch processed",
		"success", len(successIDs),
		"failed", len(events)-len(successIDs),
	)
	return nil
}

func (w *Worker) runWorker(ctx context.Context, jobs <-chan Event, processedIDs chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-jobs:
			if !ok {
				return
			}
			if err := w.publisher.Publish(ctx, event); err != nil {
				w.logger.Error("publish failed",
					"event_id", event.ID,
					"err", err,
				)
				if markErr := w.repo.MarkAsFailed(ctx, event.ID); markErr != nil {
					w.logger.Error("mark as failed", "event_id", event.ID, "err", markErr)
				}
				continue
			}

			select {
			case <-ctx.Done():
				return
			case processedIDs <- event.ID:
			}
		}
	}
}
