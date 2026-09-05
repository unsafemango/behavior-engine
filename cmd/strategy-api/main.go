// Command strategy-api serves the personalization and A/B testing
// surface: given a user id, it returns which UI variant/promo/content to
// show (backed by the strategy engine over that user's profile), and it
// exposes deterministic experiment bucketing plus exposure/conversion
// tracking and a results readout.
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

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/unsafemango/behavior-engine/internal/config"
	"github.com/unsafemango/behavior-engine/internal/experiment"
	"github.com/unsafemango/behavior-engine/internal/httpmw"
	"github.com/unsafemango/behavior-engine/internal/logging"
	"github.com/unsafemango/behavior-engine/internal/metrics"
	"github.com/unsafemango/behavior-engine/internal/profile"
	"github.com/unsafemango/behavior-engine/internal/strategy"
)

const serviceName = "strategy-api"

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

	h := &handler{
		pgStore:    profile.NewPostgresStore(pgPool),
		cache:      profile.NewRedisCache(redisClient, cfg.ProfileCacheTTL),
		expStore:   experiment.NewStore(pgPool),
		activeExpt: cfg.ActiveExperimentID,
		log:        log,
	}

	r := chi.NewRouter()
	r.Use(httpmw.RequestID)
	r.Use(httpmw.Recoverer(log))
	r.Use(httpmw.Metrics(serviceName))
	r.Use(httpmw.AccessLog(log))

	r.Get("/healthz", h.healthz)
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) { h.readyz(w, r, pgPool, redisClient) })
	r.Handle("/metrics", metrics.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Get("/strategy/{userID}", h.getStrategy)
		r.Get("/profiles/{userID}", h.getProfile)

		r.Route("/experiments/{experimentID}", func(r chi.Router) {
			r.Get("/assign/{userID}", h.assign)
			r.Post("/expose", h.expose)
			r.Post("/convert", h.convert)
			r.Get("/results", h.results)
		})
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: r, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server_error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting_down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type handler struct {
	pgStore    *profile.PostgresStore
	cache      *profile.RedisCache
	expStore   *experiment.Store
	activeExpt string
	log        *slog.Logger
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *handler) readyz(w http.ResponseWriter, r *http.Request, pgPool *pgxpool.Pool, redisClient *redis.Client) {
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
}

// loadProfile reads through Redis first, falling back to Postgres on a
// miss (and repopulating Redis so the next read is fast again). This is
// the standard cache-aside pattern.
func (h *handler) loadProfile(ctx context.Context, userID string) (profile.Profile, error) {
	if p, ok, err := h.cache.Get(ctx, userID); err != nil {
		h.log.Warn("redis_get_failed", "error", err, "user_id", userID)
	} else if ok {
		metrics.StrategyCacheResult.WithLabelValues("redis_hit").Inc()
		return p, nil
	}

	p, err := h.pgStore.Get(ctx, userID)
	if err != nil {
		if errors.Is(err, profile.ErrNotFound) {
			metrics.StrategyCacheResult.WithLabelValues("not_found").Inc()
		}
		return profile.Profile{}, err
	}
	metrics.StrategyCacheResult.WithLabelValues("redis_miss_postgres_hit").Inc()

	if err := h.cache.Set(ctx, p); err != nil {
		h.log.Warn("redis_set_failed", "error", err, "user_id", userID)
	}
	return p, nil
}

func (h *handler) getProfile(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	p, err := h.loadProfile(r.Context(), userID)
	if err != nil {
		if errors.Is(err, profile.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "no profile for this user yet")
			return
		}
		h.log.Error("load_profile_failed", "error", err, "user_id", userID)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to load profile")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// getStrategy is the core personalization endpoint: profile -> decision,
// plus this user's bucket in the configured active experiment (exposure
// recorded as a side effect, matching how a real product would fire
// exposure tracking at the moment a variant is actually shown).
func (h *handler) getStrategy(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	ctx := r.Context()

	p, err := h.loadProfile(ctx, userID)
	if err != nil && !errors.Is(err, profile.ErrNotFound) {
		h.log.Error("load_profile_failed", "error", err, "user_id", userID)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to load profile")
		return
	}
	// A user with no events yet still gets a decision (treated as brand
	// new) rather than a 404 -- the ingestion pipeline may just not have
	// caught up, and "new user" is the right default anyway.
	if errors.Is(err, profile.ErrNotFound) {
		p = profile.Profile{UserID: userID}
	}

	decision := strategy.Decide(p, time.Now())

	resp := map[string]any{
		"user_id":  userID,
		"decision": decision,
	}

	if exp, err := h.expStore.Get(ctx, h.activeExpt); err == nil && exp.Active {
		if variant, ok := experiment.Assign(h.activeExpt, userID, exp.Variants); ok {
			if err := h.expStore.RecordExposure(ctx, h.activeExpt, userID, variant); err != nil {
				h.log.Warn("record_exposure_failed", "error", err, "user_id", userID)
			} else {
				metrics.ExperimentExposuresTotal.WithLabelValues(h.activeExpt, variant).Inc()
			}
			resp["experiment"] = map[string]string{"id": h.activeExpt, "variant": variant}
		}
	} else if err != nil && !errors.Is(err, experiment.ErrExperimentNotFound) {
		h.log.Warn("load_experiment_failed", "error", err)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *handler) assign(w http.ResponseWriter, r *http.Request) {
	experimentID := chi.URLParam(r, "experimentID")
	userID := chi.URLParam(r, "userID")

	exp, err := h.expStore.Get(r.Context(), experimentID)
	if err != nil {
		if errors.Is(err, experiment.ErrExperimentNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "unknown experiment")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to load experiment")
		return
	}

	variant, ok := experiment.Assign(experimentID, userID, exp.Variants)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "no_variants", "experiment has no active variants")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"experiment_id": experimentID, "user_id": userID, "variant": variant})
}

type exposeRequest struct {
	UserID string `json:"user_id"`
}

func (h *handler) expose(w http.ResponseWriter, r *http.Request) {
	experimentID := chi.URLParam(r, "experimentID")
	var req exposeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_id is required")
		return
	}

	exp, err := h.expStore.Get(r.Context(), experimentID)
	if err != nil {
		if errors.Is(err, experiment.ErrExperimentNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "unknown experiment")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to load experiment")
		return
	}
	variant, ok := experiment.Assign(experimentID, req.UserID, exp.Variants)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "no_variants", "experiment has no active variants")
		return
	}
	if err := h.expStore.RecordExposure(r.Context(), experimentID, req.UserID, variant); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to record exposure")
		return
	}
	metrics.ExperimentExposuresTotal.WithLabelValues(experimentID, variant).Inc()
	writeJSON(w, http.StatusOK, map[string]string{"experiment_id": experimentID, "user_id": req.UserID, "variant": variant})
}

type convertRequest struct {
	UserID string  `json:"user_id"`
	Value  float64 `json:"value,omitempty"`
}

func (h *handler) convert(w http.ResponseWriter, r *http.Request) {
	experimentID := chi.URLParam(r, "experimentID")
	var req convertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_id is required")
		return
	}
	if req.Value == 0 {
		req.Value = 1
	}

	variant, err := h.expStore.ExposureVariant(r.Context(), experimentID, req.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to look up exposure")
		return
	}
	if variant == "" {
		writeError(w, http.StatusConflict, "no_exposure", "user was never exposed to this experiment")
		return
	}
	if err := h.expStore.RecordConversion(r.Context(), experimentID, req.UserID, variant, req.Value); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to record conversion")
		return
	}
	metrics.ExperimentConversionsTotal.WithLabelValues(experimentID, variant).Inc()
	writeJSON(w, http.StatusOK, map[string]string{"experiment_id": experimentID, "user_id": req.UserID, "variant": variant})
}

func (h *handler) results(w http.ResponseWriter, r *http.Request) {
	experimentID := chi.URLParam(r, "experimentID")
	res, err := h.expStore.Results(r.Context(), experimentID)
	if err != nil {
		if errors.Is(err, experiment.ErrExperimentNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "unknown experiment")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to compute results")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}
