// Package httpmw holds the small set of HTTP middlewares shared by all
// three services: request-id propagation, structured access logging,
// panic recovery, and a Prometheus metrics wrapper keyed off chi's
// matched route pattern (so label cardinality stays bounded even under
// load-test traffic hitting /v1/strategy/{userID} with thousands of
// distinct user ids).
package httpmw

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/unsafemango/behavior-engine/internal/logging"
	"github.com/unsafemango/behavior-engine/internal/metrics"
)

// RequestID assigns a request id (from X-Request-ID if the caller sent
// one, otherwise a fresh UUID) and stores it on the request context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := logging.WithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// AccessLog logs one structured line per request: method, route, status,
// duration and request id. Must run after RequestID in the chain.
func AccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			log := logging.FromContext(r.Context(), logger)
			log.Info("http_request",
				"method", r.Method,
				"path", r.URL.Path,
				"route", RoutePattern(r),
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

// Recoverer converts a panic in a downstream handler into a 500 response
// and a logged error instead of crashing the process.
func Recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log := logging.FromContext(r.Context(), logger)
					log.Error("panic_recovered", "error", rec, "path", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal_server_error"}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RoutePattern returns chi's matched route template (e.g.
// "/v1/strategy/{userID}") rather than the raw path, so metrics and logs
// don't explode in cardinality. Falls back to the raw path if chi has no
// route context (shouldn't happen inside a chi.Mux, but keeps this safe
// to reuse standalone).
func RoutePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}
	return r.URL.Path
}

// Metrics wraps metrics.Middleware with RoutePattern as the label source.
func Metrics(service string) func(http.Handler) http.Handler {
	return metrics.Middleware(service, RoutePattern)
}
