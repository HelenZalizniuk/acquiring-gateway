package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// ProcessedStore deduplicates consumed events by event_id.
type ProcessedStore struct {
	pool *pgxpool.Pool
}

func NewProcessedStore(pool *pgxpool.Pool) *ProcessedStore {
	return &ProcessedStore{pool: pool}
}

// TryBegin returns true when event_id is new and may be processed.
// Duplicate deliveries return false.
func (s *ProcessedStore) TryBegin(ctx context.Context, eventID, eventType string) (bool, error) {
	const query = `
		INSERT INTO processed_events (event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
	`
	tag, err := s.pool.Exec(ctx, query, eventID, eventType)
	if err != nil {
		return false, fmt.Errorf("claim processed event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RollbackClaim removes the claim so a failed handler can be retried.
func (s *ProcessedStore) RollbackClaim(ctx context.Context, eventID string) error {
	const query = `DELETE FROM processed_events WHERE event_id = $1`
	if _, err := s.pool.Exec(ctx, query, eventID); err != nil {
		return fmt.Errorf("rollback processed claim: %w", err)
	}
	return nil
}

func (s *ProcessedStore) Exists(ctx context.Context, eventID string) (bool, error) {
	const query = `SELECT 1 FROM processed_events WHERE event_id = $1`
	var one int
	err := s.pool.QueryRow(ctx, query, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type EventHandler func(ctx context.Context, msg Message) error

type Message struct {
	EventID       string          `json:"event_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

type Config struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

const (
	defaultKafkaBroker    = "localhost:9092"
	defaultKafkaTopic     = "payments.events"
	defaultKafkaGroupID   = "payment-event-consumers"
	defaultMaxRetries     = 5
	defaultInitialBackoff = 200 * time.Millisecond
	defaultMaxBackoff     = 10 * time.Second
	defaultMinBytes       = 1
	defaultMaxBytes       = 10e6
)

func DefaultConfig() Config {
	return Config{
		Brokers:        []string{defaultKafkaBroker},
		Topic:          defaultKafkaTopic,
		GroupID:        defaultKafkaGroupID,
		MaxRetries:     defaultMaxRetries,
		InitialBackoff: defaultInitialBackoff,
		MaxBackoff:     defaultMaxBackoff,
	}
}

type Consumer struct {
	reader  *kafka.Reader
	store   *ProcessedStore
	handler EventHandler
	cfg     Config
	logger  *slog.Logger
}

func New(cfg Config, store *ProcessedStore, handler EventHandler, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	d := DefaultConfig()
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = d.Brokers
	}
	if cfg.Topic == "" {
		cfg.Topic = d.Topic
	}
	if cfg.GroupID == "" {
		cfg.GroupID = d.GroupID
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = d.MaxRetries
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = d.InitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = d.MaxBackoff
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       defaultMinBytes,
		MaxBytes:       defaultMaxBytes,
		CommitInterval: 0, // manual commit after successful processing
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader:  r,
		store:   store,
		handler: handler,
		cfg:     cfg,
		logger:  logger,
	}
}

func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("kafka consumer started",
		"topic", c.cfg.Topic,
		"group_id", c.cfg.GroupID,
		"brokers", c.cfg.Brokers,
	)

	for {
		if err := ctx.Err(); err != nil {
			c.logger.Info("kafka consumer stopping")
			return err
		}

		kmsg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error("fetch message failed", "err", err)
			if !sleepBackoff(ctx, c.cfg.InitialBackoff) {
				return ctx.Err()
			}
			continue
		}

		if err := c.processWithRetry(ctx, kmsg); err != nil {
			c.logger.Error("message processing exhausted retries",
				"offset", kmsg.Offset,
				"partition", kmsg.Partition,
				"err", err,
			)
			// Leave the offset uncommitted so the message is retried after restart or rebalance.
			continue
		}

		if err := c.reader.CommitMessages(ctx, kmsg); err != nil {
			c.logger.Error("commit offset failed", "err", err, "offset", kmsg.Offset)
		}
	}
}

func (c *Consumer) processWithRetry(ctx context.Context, kmsg kafka.Message) error {
	var msg Message
	if err := json.Unmarshal(kmsg.Value, &msg); err != nil {
		// Drop poison-pill payloads after logging so a bad message cannot stall the partition.
		c.logger.Error("invalid message payload, skipping", "err", err, "offset", kmsg.Offset)
		return nil
	}
	if msg.EventID == "" {
		for _, h := range kmsg.Headers {
			if h.Key == "event_id" && len(h.Value) > 0 {
				msg.EventID = string(h.Value)
			}
		}
	}
	if msg.EventID == "" {
		c.logger.Error("message without event_id, skipping", "offset", kmsg.Offset)
		return nil
	}

	claimed, err := c.store.TryBegin(ctx, msg.EventID, msg.EventType)
	if err != nil {
		return fmt.Errorf("idempotency claim: %w", err)
	}
	if !claimed {
		c.logger.Info("duplicate event skipped",
			"event_id", msg.EventID,
			"event_type", msg.EventType,
		)
		return nil
	}

	backoff := c.cfg.InitialBackoff
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		lastErr = c.handler(ctx, msg)
		if lastErr == nil {
			c.logger.Info("event processed",
				"event_id", msg.EventID,
				"event_type", msg.EventType,
				"aggregate_id", msg.AggregateID,
				"attempt", attempt,
			)
			return nil
		}

		c.logger.Warn("handler failed, will retry",
			"event_id", msg.EventID,
			"attempt", attempt,
			"max_retries", c.cfg.MaxRetries,
			"err", lastErr,
		)

		if attempt == c.cfg.MaxRetries {
			break
		}
		if !sleepBackoff(ctx, backoff) {
			_ = c.store.RollbackClaim(ctx, msg.EventID)
			return ctx.Err()
		}
		backoff = nextBackoff(backoff, c.cfg.MaxBackoff)
	}

	// Retries exhausted: drop the claim so a later delivery can process the event.
	if rbErr := c.store.RollbackClaim(ctx, msg.EventID); rbErr != nil {
		c.logger.Error("rollback claim failed", "event_id", msg.EventID, "err", rbErr)
	}
	return lastErr
}

func sleepBackoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
