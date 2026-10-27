package gateway

import (
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCClientConfig holds timeouts and retry/backoff for payment gRPC calls.
type GRPCClientConfig struct {
	// CallTimeout is the per-RPC deadline, including client retries.
	CallTimeout time.Duration
	// MaxAttempts includes the first try. Minimum is 1.
	MaxAttempts int
	// InitialBackoff is the first delay between retries.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential backoff.
	MaxBackoff time.Duration
	// BackoffMultiplier is the exponential backoff factor.
	BackoffMultiplier float64
}

const (
	defaultGRPCCallTimeout    = 15 * time.Second
	defaultGRPCMaxAttempts    = 4
	defaultGRPCInitialBackoff = 100 * time.Millisecond
	defaultGRPCMaxBackoff     = 2 * time.Second
	defaultGRPCBackoffFactor  = 2.0
	defaultGRPCKeepaliveTime  = 30 * time.Second
	defaultGRPCKeepaliveWait  = 10 * time.Second
)

func DefaultGRPCClientConfig() GRPCClientConfig {
	return GRPCClientConfig{
		CallTimeout:       defaultGRPCCallTimeout,
		MaxAttempts:       defaultGRPCMaxAttempts,
		InitialBackoff:    defaultGRPCInitialBackoff,
		MaxBackoff:        defaultGRPCMaxBackoff,
		BackoffMultiplier: defaultGRPCBackoffFactor,
	}
}

func (c GRPCClientConfig) withDefaults() GRPCClientConfig {
	d := DefaultGRPCClientConfig()
	if c.CallTimeout <= 0 {
		c.CallTimeout = d.CallTimeout
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = d.InitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.BackoffMultiplier <= 0 {
		c.BackoffMultiplier = d.BackoffMultiplier
	}
	return c
}

// defaultServiceConfig enables client-side retries for transient gRPC codes.
func (c GRPCClientConfig) defaultServiceConfig() string {
	c = c.withDefaults()
	return fmt.Sprintf(`{
  "methodConfig": [{
    "name": [{"service": "payment.v1.PaymentService"}],
    "waitForReady": true,
    "timeout": "%s",
    "retryPolicy": {
      "maxAttempts": %d,
      "initialBackoff": "%s",
      "maxBackoff": "%s",
      "backoffMultiplier": %g,
      "retryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED"]
    }
  }]
}`, durationToProto(c.CallTimeout), c.MaxAttempts, durationToProto(c.InitialBackoff), durationToProto(c.MaxBackoff), c.BackoffMultiplier)
}

func durationToProto(d time.Duration) string {
	// gRPC service config expects duration strings such as "0.100s" or "15s".
	if d < time.Second {
		return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
	}
	return fmt.Sprintf("%gs", d.Seconds())
}

func dialPaymentGRPC(target string, cfg GRPCClientConfig) (*grpc.ClientConn, error) {
	cfg = cfg.withDefaults()
	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(cfg.defaultServiceConfig()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                defaultGRPCKeepaliveTime,
			Timeout:             defaultGRPCKeepaliveWait,
			PermitWithoutStream: true,
		}),
	)
}
