// Command profile-consumer reads behavior events off Kafka and folds
// each one into the user's profile: an authoritative write to Postgres,
// then a refresh of the fast-lookup copy in Redis. It also serves a
// small HTTP endpoint of its own (/healthz, /metrics) so it fits the
// same operational pattern as the other two services even though it
// isn't request-driven.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/unsafemango/behavior-engine/internal/config"
	"github.com/unsafemango/behavior-engine/internal/events"
	"github.com/unsafemango/behavior-engine/internal/kafkax"
	"github.com/unsafemango/behavior-engine/internal/logging"
	"github.com/unsafemango/behavior-engine/internal/metrics"
	"github.com/unsafemango/behavior-engine/internal/profile"
)

const serviceName = "profile-consumer"

func main() {
	cfg := config.Load()
	log := logging.New(serviceName, cfg.LogLevel)
	log.Info("starting", "config", cfg.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pgPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("postgres_connect_failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	defer redisClient.Close()

	pgStore := profile.NewPostgresStore(pgPool)
	cache := profile.NewRedisCache(redisClient, cfg.ProfileCacheTTL)

	consumer := kafkax.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	// Minimal HTTP surface: liveness/readiness + Prometheus scrape.
	go serveHealth(log, cfg, pgPool, redisClient)

	log.Info("consuming", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	runLoop(ctx, log, consumer, pgStore, cache)
	log.Info("shut_down_cleanly")
}

func runLoop(ctx context.Context, log *slog.Logger, consumer *kafkax.Consumer, pgStore *profile.PostgresStore, cache *profile.RedisCache) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error("fetch_failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		var e events.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			// Malformed message: log and move on rather than blocking the
			// whole partition forever on one bad record.
			log.Error("decode_failed", "error", err, "offset", msg.Offset)
			_ = consumer.CommitMessage(ctx, msg)
			continue
		}

		if err := process(ctx, pgStore, cache, e); err != nil {
			log.Error("process_failed", "error", err, "user_id", e.UserID, "offset", msg.Offset)
			// Don't commit: this message will be redelivered. At-least-once
			// delivery means downstream writes (ApplyEvent) must stay
			// idempotent-safe under replay, which they are here since we
			// recompute aggregates from the stored counters, not re-derive
			// from a replayed log.
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err := consumer.CommitMessage(ctx, msg); err != nil {
			log.Error("commit_failed", "error", err, "offset", msg.Offset)
		}

		metrics.EventsConsumedTotal.WithLabelValues(string(e.Type)).Inc()
	}
}

func process(ctx context.Context, pgStore *profile.PostgresStore, cache *profile.RedisCache, e events.Event) error {
	start := time.Now()
	updated, err := pgStore.ApplyEvent(ctx, e)
	metrics.ProfileUpdateDuration.WithLabelValues("postgres").Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}

	start = time.Now()
	err = cache.Set(ctx, updated)
	metrics.ProfileUpdateDuration.WithLabelValues("redis").Observe(time.Since(start).Seconds())
	return err
}

func serveHealth(log *slog.Logger, cfg config.Config, pgPool *pgxpool.Pool, redisClient *redis.Client) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := pgPool.Ping(ctx); err != nil {
			http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := redisClient.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.Handle("/metrics", metrics.Handler())

	log.Info("health_listening", "addr", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil { //nolint:gosec // internal health endpoint, no need for hardened timeouts
		log.Error("health_server_error", "error", err)
	}
}
