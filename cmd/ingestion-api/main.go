// Command ingestion-api is the edge HTTP service: it accepts behavior
// events (page views, clicks, purchases, lifecycle signals) over a REST
// API, validates them, and publishes them to Kafka. It does not touch
// Postgres or Redis directly -- keeping the write path to one hop keeps
// it fast and lets it scale independently of the consumer.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/unsafemango/behavior-engine/internal/config"
	"github.com/unsafemango/behavior-engine/internal/events"
	"github.com/unsafemango/behavior-engine/internal/httpmw"
	"github.com/unsafemango/behavior-engine/internal/kafkax"
	"github.com/unsafemango/behavior-engine/internal/logging"
	"github.com/unsafemango/behavior-engine/internal/metrics"
)

const serviceName = "ingestion-api"

// maxBatchSize bounds a single /v1/events/batch request so one caller
// can't send an unbounded body and stall the Kafka writer.
const maxBatchSize = 500

func main() {
	cfg := config.Load()
	log := logging.New(serviceName, cfg.LogLevel)
	log.Info("starting", "config", cfg.String())

	producer := kafkax.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	h := &handler{producer: producer, topic: cfg.KafkaTopic, log: log}

	r := chi.NewRouter()
	r.Use(httpmw.RequestID)
	r.Use(httpmw.Recoverer(log))
	r.Use(httpmw.Metrics(serviceName))
	r.Use(httpmw.AccessLog(log))

	r.Get("/healthz", h.healthz)
	r.Get("/readyz", h.healthz) // no external deps to check beyond process liveness
	r.Handle("/metrics", metrics.Handler())

	r.Route("/v1/events", func(r chi.Router) {
		r.Post("/", h.postEvent)
		r.Post("/batch", h.postEventBatch)
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server_error", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(log, srv)
}

func waitForShutdown(log *slog.Logger, srv *http.Server) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting_down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("shutdown_error", "error", err)
	}
}

type handler struct {
	producer *kafkax.Producer
	topic    string
	log      *slog.Logger
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *handler) postEvent(w http.ResponseWriter, r *http.Request) {
	var raw events.Event
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	e, err := events.Normalize(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", err.Error())
		return
	}

	if err := h.publish(r.Context(), e); err != nil {
		h.log.Error("publish_failed", "error", err, "user_id", e.UserID)
		writeError(w, http.StatusServiceUnavailable, "publish_failed", "could not publish event")
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"event_id": e.EventID, "status": "accepted"})
}

func (h *handler) postEventBatch(w http.ResponseWriter, r *http.Request) {
	var raw []events.Event
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if len(raw) == 0 {
		writeError(w, http.StatusBadRequest, "empty_batch", "batch must contain at least one event")
		return
	}
	if len(raw) > maxBatchSize {
		writeError(w, http.StatusBadRequest, "batch_too_large", "batch exceeds maximum size")
		return
	}

	accepted := make([]string, 0, len(raw))
	for i, r0 := range raw {
		e, err := events.Normalize(r0)
		if err != nil {
			writeError(w, http.StatusBadRequest, "validation_failed", err.Error()+" at index "+strconv.Itoa(i))
			return
		}
		if err := h.publish(r.Context(), e); err != nil {
			h.log.Error("publish_failed", "error", err, "user_id", e.UserID)
			writeError(w, http.StatusServiceUnavailable, "publish_failed", "could not publish batch")
			return
		}
		accepted = append(accepted, e.EventID)
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"accepted": accepted, "count": len(accepted)})
}

func (h *handler) publish(ctx context.Context, e events.Event) error {
	start := time.Now()
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	err = h.producer.Publish(ctx, e.UserID, body)
	metrics.KafkaPublishDuration.WithLabelValues(h.topic).Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.KafkaPublishTotal.WithLabelValues(h.topic, "error").Inc()
		return err
	}
	metrics.KafkaPublishTotal.WithLabelValues(h.topic, "ok").Inc()
	return nil
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": message})
}
