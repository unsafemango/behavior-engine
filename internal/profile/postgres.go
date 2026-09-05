package profile

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unsafemango/behavior-engine/internal/events"
)

// ErrNotFound is returned by PostgresStore.Get when no profile exists
// for the given user id.
var ErrNotFound = errors.New("profile not found")

// PostgresStore is the durable source of truth for profiles. The
// consumer is the only writer; the strategy API reads through it only
// on a Redis cache miss.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// ApplyEvent atomically loads the current row (if any), folds the event
// in with ApplyEvent, and upserts the result -- all inside one
// transaction so concurrent events for the same user (unlikely given
// Kafka's per-key partitioning, but possible during a rebalance) can't
// race and lose an update.
func (s *PostgresStore) ApplyEvent(ctx context.Context, e events.Event) (Profile, error) {
	var result Profile

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		current, err := getForUpdate(ctx, tx, e.UserID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		updated := ApplyEvent(current, e) // zero-value Profile{} on first event, per ApplyEvent's contract
		if err := upsert(ctx, tx, updated); err != nil {
			return err
		}
		result = updated
		return nil
	})
	if err != nil {
		return Profile{}, err
	}
	return result, nil
}

func getForUpdate(ctx context.Context, tx pgx.Tx, userID string) (Profile, error) {
	row := tx.QueryRow(ctx, `
		SELECT user_id, first_seen_at, last_event_at, total_events, page_views,
		       clicks, purchases, lifecycle_events, engagement_score, lifecycle_stage, updated_at
		FROM user_profiles WHERE user_id = $1 FOR UPDATE`, userID)
	return scanProfile(row)
}

// Get reads the current profile without locking -- used by the strategy
// API on a Redis cache miss.
func (s *PostgresStore) Get(ctx context.Context, userID string) (Profile, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT user_id, first_seen_at, last_event_at, total_events, page_views,
		       clicks, purchases, lifecycle_events, engagement_score, lifecycle_stage, updated_at
		FROM user_profiles WHERE user_id = $1`, userID)
	return scanProfile(row)
}

func scanProfile(row pgx.Row) (Profile, error) {
	var p Profile
	var stage string
	err := row.Scan(&p.UserID, &p.FirstSeenAt, &p.LastEventAt, &p.TotalEvents, &p.PageViews,
		&p.Clicks, &p.Purchases, &p.LifecycleEvents, &p.EngagementScore, &stage, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, err
	}
	p.LifecycleStage = LifecycleStage(stage)
	return p, nil
}

func upsert(ctx context.Context, tx pgx.Tx, p Profile) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO user_profiles (
			user_id, first_seen_at, last_event_at, total_events, page_views,
			clicks, purchases, lifecycle_events, engagement_score, lifecycle_stage, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (user_id) DO UPDATE SET
			first_seen_at    = LEAST(user_profiles.first_seen_at, EXCLUDED.first_seen_at),
			last_event_at    = GREATEST(user_profiles.last_event_at, EXCLUDED.last_event_at),
			total_events     = EXCLUDED.total_events,
			page_views       = EXCLUDED.page_views,
			clicks           = EXCLUDED.clicks,
			purchases        = EXCLUDED.purchases,
			lifecycle_events = EXCLUDED.lifecycle_events,
			engagement_score = EXCLUDED.engagement_score,
			lifecycle_stage  = EXCLUDED.lifecycle_stage,
			updated_at       = EXCLUDED.updated_at`,
		p.UserID, p.FirstSeenAt, p.LastEventAt, p.TotalEvents, p.PageViews,
		p.Clicks, p.Purchases, p.LifecycleEvents, p.EngagementScore, string(p.LifecycleStage), p.UpdatedAt,
	)
	return err
}

// Ping is used by the readiness probe.
func (s *PostgresStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.pool.Ping(ctx)
}
