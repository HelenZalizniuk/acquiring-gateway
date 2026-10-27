package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"acquiring-gateway/internal/config"
	"acquiring-gateway/internal/db"
	kafkaconsumer "acquiring-gateway/internal/paymentsvc/kafkaconsumer"
	"acquiring-gateway/internal/paymentsvc/repository"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, config.EnvOr("DATABASE_URL", config.DefaultDatabaseURL))
	if err != nil {
		logger.Error("connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := kafkaconsumer.NewProcessedStore(pool)
	cfg := kafkaconsumer.Config{
		Brokers: config.EnvCSV("KAFKA_BROKERS", config.DefaultKafkaBrokers),
		Topic:   config.EnvOr("KAFKA_TOPIC", config.DefaultKafkaTopic),
		GroupID: config.EnvOr("KAFKA_GROUP_ID", config.DefaultKafkaGroupID),
	}

	handler := func(ctx context.Context, msg kafkaconsumer.Message) error {
		logger.Info("handling payment event",
			"event_id", msg.EventID,
			"event_type", msg.EventType,
			"aggregate_id", msg.AggregateID,
		)
		switch msg.EventType {
		case repository.EventTypeCreated, repository.EventTypeCompleted, repository.EventTypeCompensated:
			return nil
		default:
			logger.Warn("unknown event type, ack as processed", "event_type", msg.EventType)
			return nil
		}
	}

	consumer := kafkaconsumer.New(cfg, store, handler, logger)
	defer consumer.Close()

	logger.Info("payment kafka consumer starting", "brokers", cfg.Brokers, "topic", cfg.Topic)
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
	logger.Info("consumer stopped")
}
