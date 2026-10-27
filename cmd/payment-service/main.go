package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	paymentv1 "acquiring-gateway/gen/go/payment/v1"
	"acquiring-gateway/internal/config"
	"acquiring-gateway/internal/db"
	"acquiring-gateway/internal/paymentsvc/repository"
	"acquiring-gateway/internal/paymentsvc/service"
	paymenttemporal "acquiring-gateway/internal/paymentsvc/temporal"
	grpcserver "acquiring-gateway/internal/paymentsvc/transport/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const shutdownTimeout = 10 * time.Second

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
	logger.Info("connected to PostgreSQL")

	repo := repository.NewPaymentRepository(pool)
	svc := service.NewPaymentService(repo)

	temporalCfg := paymenttemporal.WorkerConfig{
		HostPort:  config.EnvOr("TEMPORAL_HOST_PORT", config.DefaultTemporalHostPort),
		Namespace: config.EnvOr("TEMPORAL_NAMESPACE", config.DefaultTemporalNS),
		Queue:     config.EnvOr("TEMPORAL_QUEUE", paymenttemporal.PaymentQueue),
	}

	temporalClient, err := paymenttemporal.NewClient(temporalCfg, logger)
	if err != nil {
		logger.Error("connect to temporal", "err", err, "host_port", temporalCfg.HostPort)
		os.Exit(1)
	}
	defer temporalClient.Close()
	logger.Info("connected to Temporal", "host_port", temporalCfg.HostPort)

	go func() {
		if err := paymenttemporal.RunWorker(ctx, temporalCfg, svc, logger); err != nil {
			logger.Error("temporal worker stopped", "err", err)
			stop()
		}
	}()

	paymentServer := grpcserver.NewPaymentServer(svc, logger).WithTemporal(temporalClient, temporalCfg.Queue)

	grpcServer := grpc.NewServer()
	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentServer)
	reflection.Register(grpcServer)

	addr := config.EnvOr("PAYMENT_GRPC_ADDR", config.DefaultPaymentGRPCAddr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listen", "addr", addr, "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("payment gRPC server started", "addr", addr)
		errCh <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("gRPC server failed", "err", err)
			os.Exit(1)
		}
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("payment gRPC server stopped gracefully")
	case <-time.After(shutdownTimeout):
		logger.Warn("graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}
}
