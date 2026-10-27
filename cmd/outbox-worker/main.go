package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"acquiring-gateway/internal/config"
	"acquiring-gateway/internal/db"
	"acquiring-gateway/internal/paymentsvc/outbox"
)

const shutdownTimeout = 5 * time.Second

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

	brokers := config.EnvCSV("KAFKA_BROKERS", config.DefaultKafkaBrokers)
	topic := config.EnvOr("KAFKA_TOPIC", config.DefaultKafkaTopic)

	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Brokers: brokers,
		Topic:   topic,
	}, logger)
	defer pub.Close()

	pprofAddr := config.EnvOr("PPROF_ADDR", config.DefaultPprofAddr)
	pprofSrv := &http.Server{Addr: pprofAddr}
	go func() {
		logger.Info("pprof server started", "addr", pprofAddr)
		if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("pprof server", "err", err)
		}
	}()

	defaults := outbox.DefaultConfig()
	workerCfg := outbox.Config{
		WorkersCount:   config.EnvInt("OUTBOX_WORKERS", defaults.WorkersCount),
		BatchSize:      config.EnvInt("OUTBOX_BATCH_SIZE", defaults.BatchSize),
		PollInterval:   config.EnvDuration("OUTBOX_POLL_INTERVAL", defaults.PollInterval),
		StaleOlderThan: config.EnvDuration("OUTBOX_STALE_AFTER", defaults.StaleOlderThan),
	}

	repo := outbox.NewRepository(pool)
	worker := outbox.NewWorker(repo, pub, workerCfg, logger)

	logger.Info("outbox worker process starting", "brokers", brokers, "topic", topic)
	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("outbox worker stopped", "err", err)
		os.Exit(1)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := pprofSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("pprof shutdown failed", "err", err)
	}

	logger.Info("outbox worker stopped")
}
