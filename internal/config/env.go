package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultDatabaseURL      = "postgres://postgres:postgres@localhost:5432/acquiring?sslmode=disable"
	DefaultGatewayAddr      = ":8000"
	DefaultHTTPAddr         = ":8080"
	DefaultLegacyURL        = "http://localhost:8080"
	DefaultPaymentGRPCAddr  = ":8081"
	DefaultNewPaymentsURL   = "localhost:8081"
	DefaultPprofAddr        = ":6060"
	DefaultKafkaBrokers     = "localhost:9092"
	DefaultKafkaTopic       = "payments.events"
	DefaultKafkaGroupID     = "payment-event-consumers"
	DefaultTemporalHostPort = "localhost:7233"
	DefaultTemporalNS       = "default"
)

func EnvOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func EnvCSV(key, fallback string) []string {
	return splitCSV(EnvOr(key, fallback))
}

func EnvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func EnvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
