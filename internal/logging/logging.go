// Package logging sets up structured (JSON) logging via the standard
// library slog package. Every service uses this so log lines are uniform
// and easy to ship to any log aggregator without a third-party dependency.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

// New builds a JSON slog.Logger at the given level ("debug", "info",
// "warn", "error") tagged with the service name on every line.
func New(service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service)
}

// WithRequestID stores a request id on the context so it can be attached
// to every log line emitted while handling that request.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request id stashed by WithRequestID,
// or "" if none is set.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// FromContext returns a logger enriched with the request id from ctx, if
// any. Handlers should call this instead of using the base logger
// directly so every log line during a request is correlated.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := RequestIDFromContext(ctx); id != "" {
		return base.With("request_id", id)
	}
	return base
}
