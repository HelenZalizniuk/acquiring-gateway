package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"acquiring-gateway/internal/config"
	"acquiring-gateway/internal/db"
	"acquiring-gateway/internal/repository"
	"acquiring-gateway/internal/service"
	httpTransport "acquiring-gateway/internal/transport/http"
)

func main() {
	ctx := context.Background()

	pool, err := db.NewPool(ctx, config.EnvOr("DATABASE_URL", config.DefaultDatabaseURL))
	if err != nil {
		slog.Error("connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("connected to PostgreSQL")

	repo := repository.NewPaymentRepository(pool)
	paymentSvc := service.NewPaymentService(repo)
	paymentHandler := httpTransport.NewPaymentHandler(paymentSvc)

	mux := http.NewServeMux()
	paymentHandler.RegisterRoutes(mux)

	addr := config.EnvOr("HTTP_ADDR", config.DefaultHTTPAddr)
	slog.Info("HTTP server started", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("HTTP server stopped", "err", err)
		os.Exit(1)
	}
}
