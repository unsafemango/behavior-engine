// Package metrics defines the Prometheus metrics shared across services
// and a small HTTP middleware that records request counts and latency
// for every route automatically.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP request metrics, labeled by service so all three binaries can
// share one Prometheus scrape config and one Grafana dashboard.
var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests processed, labeled by route, method and status.",
	}, []string{"service", "route", "method", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "route", "method"})

	// Kafka producer metrics (ingestion-api).
	KafkaPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_publish_total",
		Help: "Total events published to Kafka, labeled by outcome (ok|error).",
	}, []string{"topic", "outcome"})

	KafkaPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_publish_duration_seconds",
		Help:    "Latency of publishing a batch of events to Kafka.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	// Consumer metrics (profile-consumer).
	EventsConsumedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "events_consumed_total",
		Help: "Total events consumed from Kafka, labeled by event type.",
	}, []string{"event_type"})

	ProfileUpdateDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "profile_update_duration_seconds",
		Help:    "Time to apply one event to Postgres + Redis profile state.",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage"}) // stage = postgres|redis

	ConsumerLagEstimate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_lag_estimate",
		Help: "Approximate messages behind the partition high watermark, summed across assigned partitions.",
	})

	// Strategy / experiment metrics (strategy-api).
	StrategyCacheResult = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "strategy_profile_lookup_total",
		Help: "Profile lookups for strategy decisions, labeled by source (redis_hit|redis_miss_postgres_hit|not_found).",
	}, []string{"source"})

	ExperimentExposuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "experiment_exposures_total",
		Help: "Total experiment exposures recorded, labeled by experiment and variant.",
	}, []string{"experiment_id", "variant"})

	ExperimentConversionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "experiment_conversions_total",
		Help: "Total experiment conversions recorded, labeled by experiment and variant.",
	}, []string{"experiment_id", "variant"})
)

// Handler returns the standard Prometheus scrape endpoint handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// statusRecorder wraps http.ResponseWriter to capture the status code
// written by downstream handlers for the metrics middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Middleware records request count and duration for every request. route
// should be a low-cardinality template (e.g. "/v1/strategy/{userID}"),
// never the raw path, to keep Prometheus label cardinality bounded.
func Middleware(service string, routeFn func(r *http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			route := routeFn(r)
			duration := time.Since(start).Seconds()
			HTTPRequestsTotal.WithLabelValues(service, route, r.Method, strconv.Itoa(rec.status)).Inc()
			HTTPRequestDuration.WithLabelValues(service, route, r.Method).Observe(duration)
		})
	}
}
