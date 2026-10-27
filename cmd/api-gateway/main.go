package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"acquiring-gateway/internal/config"
	"acquiring-gateway/internal/gateway"
)

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 15 * time.Second
	httpWriteTimeout      = 30 * time.Second
	httpIdleTimeout       = 60 * time.Second
	shutdownTimeout       = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := gateway.Config{
		ListenAddr:        config.EnvOr("GATEWAY_ADDR", config.DefaultGatewayAddr),
		LegacyURL:         config.EnvOr("LEGACY_URL", config.DefaultLegacyURL),
		NewPaymentsURL:    config.EnvOr("NEW_PAYMENTS_URL", config.DefaultNewPaymentsURL),
		MigratedMerchants: parseMerchants(config.EnvOr("MIGRATED_MERCHANTS", "")),
		GRPC:              gateway.DefaultGRPCClientConfig(),
	}

	gw, err := gateway.New(cfg, logger)
	if err != nil {
		logger.Error("create api gateway", "err", err)
		os.Exit(1)
	}
	defer gw.Close()

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("api gateway started",
			"addr", cfg.ListenAddr,
			"legacy_url", cfg.LegacyURL,
			"new_payments_url", cfg.NewPaymentsURL,
			"migrated_merchants", len(cfg.MigratedMerchants),
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api gateway failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api gateway")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}
	logger.Info("api gateway stopped")
}

func parseMerchants(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}
