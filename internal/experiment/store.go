package experiment

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrExperimentNotFound = errors.New("experiment not found")

// Store persists experiment configs, exposures and conversions in
// Postgres. Exposures/conversions are deduplicated per (experiment,
// user) at the DB level via a unique constraint, so a user who calls
// the strategy endpoint 50 times in a session only counts once -- an
// intentional simplification documented in the README (a real platform
// would also track exposure *count* and time-windowed conversions).
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Get loads an experiment and its variants.
func (s *Store) Get(ctx context.Context, id string) (Experiment, error) {
	var exp Experiment
	exp.ID = id

	row := s.pool.QueryRow(ctx, `SELECT name, active FROM experiments WHERE id = $1`, id)
	if err := row.Scan(&exp.Name, &exp.Active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Experiment{}, ErrExperimentNotFound
		}
		return Experiment{}, err
	}

	rows, err := s.pool.Query(ctx, `SELECT variant, weight FROM experiment_variants WHERE experiment_id = $1 ORDER BY variant`, id)
	if err != nil {
		return Experiment{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var v Variant
		if err := rows.Scan(&v.Name, &v.Weight); err != nil {
			return Experiment{}, err
		}
		exp.Variants = append(exp.Variants, v)
	}
	return exp, rows.Err()
}

// RecordExposure stores that userID saw variant of experimentID. A
// duplicate exposure (same user, same experiment) is a silent no-op --
// idempotent by design, since the strategy endpoint may be called many
// times for the same user.
func (s *Store) RecordExposure(ctx context.Context, experimentID, userID, variant string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO experiment_exposures (experiment_id, user_id, variant)
		VALUES ($1, $2, $3)
		ON CONFLICT (experiment_id, user_id) DO NOTHING`,
		experimentID, userID, variant)
	return err
}

// RecordConversion stores that userID converted within experimentID.
// Requires the user to already have a recorded exposure so results
// aren't skewed by conversions from users who were never bucketed; the
// caller (HTTP handler) looks up the user's variant first.
func (s *Store) RecordConversion(ctx context.Context, experimentID, userID, variant string, value float64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO experiment_conversions (experiment_id, user_id, variant, value)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (experiment_id, user_id) DO NOTHING`,
		experimentID, userID, variant, value)
	return err
}

// ExposureVariant returns the variant userID was exposed to for
// experimentID, or "" if they've never been exposed.
func (s *Store) ExposureVariant(ctx context.Context, experimentID, userID string) (string, error) {
	var variant string
	err := s.pool.QueryRow(ctx, `
		SELECT variant FROM experiment_exposures WHERE experiment_id = $1 AND user_id = $2`,
		experimentID, userID).Scan(&variant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return variant, err
}

// Results aggregates exposures and conversions per variant and runs a
// z-test of every non-baseline variant against the first variant
// (alphabetically first configured variant, treated as control).
func (s *Store) Results(ctx context.Context, experimentID string) (Results, error) {
	exp, err := s.Get(ctx, experimentID)
	if err != nil {
		return Results{}, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			v.variant,
			COALESCE(e.exposures, 0)   AS exposures,
			COALESCE(c.conversions, 0) AS conversions
		FROM experiment_variants v
		LEFT JOIN (
			SELECT variant, COUNT(*) AS exposures
			FROM experiment_exposures WHERE experiment_id = $1 GROUP BY variant
		) e ON e.variant = v.variant
		LEFT JOIN (
			SELECT variant, COUNT(*) AS conversions
			FROM experiment_conversions WHERE experiment_id = $1 GROUP BY variant
		) c ON c.variant = v.variant
		WHERE v.experiment_id = $1
		ORDER BY v.variant`, experimentID)
	if err != nil {
		return Results{}, err
	}
	defer rows.Close()

	res := Results{ExperimentID: experimentID}
	var baseline VariantResult
	for rows.Next() {
		var vr VariantResult
		if err := rows.Scan(&vr.Variant, &vr.Exposures, &vr.Conversions); err != nil {
			return Results{}, err
		}
		if vr.Exposures > 0 {
			vr.Rate = round4(float64(vr.Conversions) / float64(vr.Exposures))
		}
		res.Variants = append(res.Variants, vr)
	}
	if err := rows.Err(); err != nil {
		return Results{}, err
	}
	_ = exp // exp.Active/Name available for future use (e.g. rejecting results for inactive experiments)

	if len(res.Variants) == 0 {
		return res, nil
	}
	baseline = res.Variants[0]
	res.Baseline = baseline.Variant
	for _, v := range res.Variants[1:] {
		z := TwoProportionZTest(baseline.Conversions, baseline.Exposures, v.Conversions, v.Exposures)
		res.VsBaseline = append(res.VsBaseline, ZTestSummary{Variant: v.Variant, Test: z})
	}
	return res, nil
}
