-- behavior-engine initial schema.
-- Applied automatically by the postgres container on first boot via
-- docker-compose's /docker-entrypoint-initdb.d mount; run manually with
-- `make migrate` (psql) against any other environment.

CREATE TABLE IF NOT EXISTS user_profiles (
    user_id          TEXT PRIMARY KEY,
    first_seen_at    TIMESTAMPTZ NOT NULL,
    last_event_at    TIMESTAMPTZ NOT NULL,
    total_events     BIGINT NOT NULL DEFAULT 0,
    page_views       BIGINT NOT NULL DEFAULT 0,
    clicks           BIGINT NOT NULL DEFAULT 0,
    purchases        BIGINT NOT NULL DEFAULT 0,
    lifecycle_events BIGINT NOT NULL DEFAULT 0,
    engagement_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    lifecycle_stage  TEXT NOT NULL DEFAULT 'new',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_lifecycle_stage ON user_profiles (lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_user_profiles_last_event_at ON user_profiles (last_event_at);

CREATE TABLE IF NOT EXISTS experiments (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    active     BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experiment_variants (
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    variant       TEXT NOT NULL,
    weight        INT NOT NULL CHECK (weight >= 0),
    PRIMARY KEY (experiment_id, variant)
);

CREATE TABLE IF NOT EXISTS experiment_exposures (
    id            BIGSERIAL PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL,
    variant       TEXT NOT NULL,
    exposed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (experiment_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_exposures_experiment_variant ON experiment_exposures (experiment_id, variant);

CREATE TABLE IF NOT EXISTS experiment_conversions (
    id            BIGSERIAL PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL,
    variant       TEXT NOT NULL,
    value         DOUBLE PRECISION NOT NULL DEFAULT 1,
    converted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (experiment_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_conversions_experiment_variant ON experiment_conversions (experiment_id, variant);

-- Seed the default experiment the strategy API is configured to bucket
-- users into (ACTIVE_EXPERIMENT_ID, see .env.example). A/B testing an
-- imaginary checkout button color is a stand-in for "any binary
-- experiment" -- swap the id/variants for a real one via SQL or a
-- future admin endpoint.
INSERT INTO experiments (id, name, active) VALUES
    ('checkout_button_color', 'Checkout button color', true)
ON CONFLICT (id) DO NOTHING;

INSERT INTO experiment_variants (experiment_id, variant, weight) VALUES
    ('checkout_button_color', 'control', 50),
    ('checkout_button_color', 'treatment', 50)
ON CONFLICT (experiment_id, variant) DO NOTHING;
