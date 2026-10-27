package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher writes outbox events to Kafka with acks=all (at-least-once).
type Publisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

type PublisherConfig struct {
	Brokers []string
	Topic   string
}

const (
	defaultPublishAttempts  = 5
	defaultPublishBatchWait = 10 * time.Millisecond
)

func NewPublisher(cfg PublisherConfig, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{}, // message key maps to a stable partition
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  defaultPublishAttempts,
		Async:        false, // wait for broker ack
		BatchTimeout: defaultPublishBatchWait,
		Compression:  kafka.Snappy,
	}
	return &Publisher{writer: w, logger: logger}
}

func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// KafkaMessage is the payload contract for the payments topic.
type KafkaMessage struct {
	EventID       string          `json:"event_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

// Publish sends an event. The message key is aggregate_id (payment_id) to preserve partition order.
// It returns an error if the broker does not acknowledge the write (acks=all).
func (p *Publisher) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(KafkaMessage{
		EventID:       event.ID,
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateID,
		EventType:     event.EventType,
		Payload:       event.Payload,
		CreatedAt:     event.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.AggregateID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(event.ID)},
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "aggregate_id", Value: []byte(event.AggregateID)},
		},
		Time: time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka write (acks=all): %w", err)
	}

	p.logger.Info("outbox event published",
		"event_id", event.ID,
		"event_type", event.EventType,
		"aggregate_id", event.AggregateID,
		"topic", p.writer.Topic,
	)
	return nil
}
