package paymenttemporal

import (
	"context"
	"fmt"
	"log/slog"

	"acquiring-gateway/internal/paymentsvc/service"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

type WorkerConfig struct {
	HostPort  string
	Namespace string
	Queue     string
}

const (
	defaultTemporalHostPort  = "localhost:7233"
	defaultTemporalNamespace = "default"
)

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		HostPort:  defaultTemporalHostPort,
		Namespace: defaultTemporalNamespace,
		Queue:     PaymentQueue,
	}
}

// RunWorker registers workflows and activities, then blocks until ctx is canceled.
func RunWorker(ctx context.Context, cfg WorkerConfig, payments *service.PaymentService, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.HostPort == "" {
		cfg.HostPort = defaultTemporalHostPort
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultTemporalNamespace
	}
	if cfg.Queue == "" {
		cfg.Queue = PaymentQueue
	}

	c, err := client.Dial(client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
		Logger:    newTemporalLogger(logger),
	})
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer c.Close()

	w := worker.New(c, cfg.Queue, worker.Options{})
	activities := &Activities{
		Payments: payments,
		Logger:   logger,
	}

	w.RegisterWorkflow(ProcessPaymentWorkflow)
	w.RegisterActivity(activities.ChargePaymentActivity)
	w.RegisterActivity(activities.SendNotificationActivity)
	w.RegisterActivity(activities.CompletePaymentActivity)
	w.RegisterActivity(activities.CompensateChargeActivity)

	logger.Info("temporal worker starting",
		"host_port", cfg.HostPort,
		"namespace", cfg.Namespace,
		"queue", cfg.Queue,
	)

	interruptCh := make(chan any, 1)
	go func() {
		<-ctx.Done()
		close(interruptCh)
	}()

	if err := w.Run(interruptCh); err != nil {
		return fmt.Errorf("temporal worker: %w", err)
	}
	logger.Info("temporal worker stopped")
	return nil
}

// NewClient dials Temporal for starting workflows from the gRPC layer.
func NewClient(cfg WorkerConfig, logger *slog.Logger) (client.Client, error) {
	if cfg.HostPort == "" {
		cfg.HostPort = defaultTemporalHostPort
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultTemporalNamespace
	}
	opts := client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	}
	if logger != nil {
		opts.Logger = newTemporalLogger(logger)
	}
	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	return c, nil
}

// StartProcessPayment starts the payment saga and waits for the result.
func StartProcessPayment(ctx context.Context, c client.Client, queue string, input PaymentWorkflowInput) (PaymentWorkflowResult, error) {
	if queue == "" {
		queue = PaymentQueue
	}

	we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       "payment-" + input.IdempotencyKey,
		TaskQueue:                queue,
		WorkflowExecutionTimeout: defaultWorkflowExecutionTimeout,
		WorkflowRunTimeout:       defaultWorkflowRunTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    defaultWorkflowRetryInterval,
			BackoffCoefficient: defaultBackoffCoefficient,
			MaximumInterval:    defaultWorkflowRetryMaxInterval,
			MaximumAttempts:    defaultWorkflowRetryAttempts,
		},
	}, ProcessPaymentWorkflow, input)
	if err != nil {
		return PaymentWorkflowResult{}, fmt.Errorf("start workflow: %w", err)
	}

	var result PaymentWorkflowResult
	if err := we.Get(ctx, &result); err != nil {
		// Temporal Get returns only an error; result is empty on failure.
		return result, fmt.Errorf("workflow execution: %w", err)
	}
	return result, nil
}
