package paymenttemporal

import (
	"log/slog"

	"go.temporal.io/sdk/log"
)

type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(l *slog.Logger) log.Logger {
	return &temporalLogger{log: l}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {
	l.log.Debug(msg, keyvals...)
}

func (l *temporalLogger) Info(msg string, keyvals ...any) {
	l.log.Info(msg, keyvals...)
}

func (l *temporalLogger) Warn(msg string, keyvals ...any) {
	l.log.Warn(msg, keyvals...)
}

func (l *temporalLogger) Error(msg string, keyvals ...any) {
	l.log.Error(msg, keyvals...)
}

func (l *temporalLogger) With(keyvals ...any) log.Logger {
	return &temporalLogger{log: l.log.With(keyvals...)}
}
