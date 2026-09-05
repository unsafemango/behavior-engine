# behavior-engine

A user-behavior ingestion, profiling, personalization and A/B testing
platform, built as a Go backend portfolio project: an HTTP ingestion API
publishing to Kafka, a consumer that builds durable user profiles in
Postgres with a Redis fast-path, a rule-based strategy engine, and a
deterministic-bucketing A/B testing layer with statistical readout.
Wrapped with Prometheus metrics, structured JSON logging, and
docker-compose bringing up the whole stack. Load-tested with `hey`
against the live stack -- real numbers below, not estimates.

## Architecture

```
                    POST /v1/events
                          |
                          v
                 +------------------+
   HTTP clients->|  ingestion-api   |----publish---->  Kafka topic
                 |  (Chi, :8080)    |                 "behavior.events"
                 +------------------+                 (keyed by user_id)
                                                              |
                                                       consumer group
                                                        "profile-consumer"
                                                              v
                                                    +--------------------+
                                                    |  profile-consumer  |
                                                    |     (:8081)        |
                                                    +--------------------+
                                                       |              |
                                                  upsert           refresh
                                                       v              v
                                                 +----------+   +----------+
                                                 | Postgres |   |  Redis   |
                                                 | (durable)|   |  (fast   |
                                                 |          |   |  lookup) |
                                                 +----------+   +----------+
                                                       ^              ^
                                                       |   cache-aside|
                                                       +------+-------+
                                                              |
                 GET /v1/strategy/{userID}                   |
                          |                                  |
                          v                                  |
                 +------------------+                        |
   HTTP clients->|  strategy-api    |------------------------+
                 |  (Chi, :8082)    |
                 +------------------+
                   |            |
             strategy       A/B testing
             decision       (bucketing, exposure/
             (rules over    conversion tracking,
              profile)      results + z-test)
                                  |
                                  v
                          Postgres (experiments,
                          exposures, conversions)
```

Three independently deployable Go binaries share four `internal/`
packages (`profile`, `experiment`, `strategy`, plus `events`/`kafkax`/
`httpmw`/`metrics`/`logging`/`config`), all wired with no framework
beyond [chi](https://github.com/go-chi/chi) for routing. No ORM --
`jackc/pgx/v5` with hand-written SQL, which keeps every query visible
and is the more common expectation in Go shops than an ORM would be.

**Why three binaries instead of one monolith:** the ingestion API's job
is "accept a POST fast and get it onto Kafka" -- it should scale
horizontally under write load and never blocks on Postgres. The
consumer's job is different (steady background processing, no HTTP
traffic to speak of). The strategy API is read-heavy and cache-optimized.
Splitting them means each can be scaled, deployed and reasoned about on
its own, at the cost of needing a message bus in between -- exactly the
tradeoff Kafka exists to make.

## Data model & design decisions

**Event schema** (`internal/events`): `event_id`, `user_id`, `type`
(`page_view` | `click` | `purchase` | `lifecycle`), `timestamp`, and a
free-form `properties` bag. Validated and defaulted at the edge
(ingestion-api) so nothing malformed ever reaches Kafka.

**Kafka partitioning**: every message is keyed by `user_id` (via
`kafka.Hash` balancer), so all of one user's events land on the same
partition and are processed **in order** by the consumer. This is what
makes the profile-builder correct without needing distributed locking.

**Engagement score & lifecycle stage** (`internal/profile/model.go`):
a weighted sum of event counts (`purchase` worth 5x a `page_view`, see
the constants for the full table), with a *lazy* recency decay
(exponential, 14-day half-life) applied at **read time**, not write
time. The stored `engagement_score` is the raw, undecayed value as of
the user's last event -- deliberately, so an idle user's row doesn't
need to be rewritten just because the clock moved. `strategy-api` calls
`Profile.CurrentEngagementScore(time.Now())` to get the actual
as-of-now score. Lifecycle stage (`new` / `active` / `at_risk` /
`churned`) is similarly re-derived at read time from `first_seen_at` /
`last_event_at`, not cached as a value that could go stale. This
lazy-decay pattern is the single most important design decision in the
project and is unit tested in `internal/profile/model_test.go`.

**Cache-aside, not write-through**: `strategy-api` reads Redis first;
on a miss it reads Postgres and repopulates Redis. `profile-consumer`
writes both on every event. This means Redis is disposable -- flush it
and the system self-heals from Postgres, which is the property you want
from a cache.

**At-least-once delivery**: the consumer commits Kafka offsets only
*after* successfully writing to Postgres and Redis. A crash mid-batch
means some events get reprocessed, not lost. `ApplyEvent` recomputes
aggregates from stored counters (not by replaying a full event log), so
this is safe by construction -- reprocessing the same event twice from a
crash-and-retry is the one edge case that isn't idempotent (it would
double-count), documented as a known limitation below.

**A/B bucketing** (`internal/experiment/bucketing.go`): `SHA-256(experiment_id
+ ":" + user_id) mod total_weight`, mapped to a variant by cumulative
weight. No storage lookup needed to know which bucket a user falls in --
same user, same experiment, same result, forever, computed independently
by any service. Exposures and conversions are deduplicated per
`(experiment_id, user_id)` at the database level (`ON CONFLICT DO
NOTHING`), so calling the strategy endpoint 50 times in one session only
counts as one exposure.

**Experiment results** (`internal/experiment/stats.go`): a standard
two-proportion z-test against a configured baseline variant, using
`math.Erf` from the standard library for the normal CDF -- no stats
dependency needed for one calculation. This is a fixed-horizon test with
no sequential-testing correction; see Limitations.

## API reference

### ingestion-api (`:8080`)

| Method | Path                | Description                                |
|--------|---------------------|---------------------------------------------|
| POST   | `/v1/events`        | Ingest one event                            |
| POST   | `/v1/events/batch`  | Ingest up to 500 events in one request       |
| GET    | `/healthz`, `/readyz` | Liveness / readiness                      |
| GET    | `/metrics`          | Prometheus scrape endpoint                   |

```bash
curl -X POST localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u-42","type":"purchase","properties":{"amount":49.99}}'
# -> 202 {"event_id":"...","status":"accepted"}
```

`type` is omitted from the response body's `properties` if empty;
`event_id` and `timestamp` are server-generated if you don't supply
them.

### strategy-api (`:8082`)

| Method | Path                                          | Description |
|--------|-----------------------------------------------|--------------|
| GET    | `/v1/profiles/{userID}`                       | Raw profile (cache-aside read) |
| GET    | `/v1/strategy/{userID}`                       | Personalization decision + experiment bucket (records exposure as a side effect) |
| GET    | `/v1/experiments/{id}/assign/{userID}`        | Pure bucketing, no side effects |
| POST   | `/v1/experiments/{id}/expose`  `{"user_id"}`  | Explicitly record an exposure |
| POST   | `/v1/experiments/{id}/convert` `{"user_id","value"?}` | Record a conversion (requires a prior exposure) |
| GET    | `/v1/experiments/{id}/results`                | Per-variant exposures/conversions/rate + z-test vs. baseline |
| GET    | `/healthz`, `/readyz`, `/metrics`             | Same as above |

```bash
curl localhost:8082/v1/strategy/u-42
# -> {"user_id":"u-42",
#     "decision":{"ui_variant":"standard","promo":"small_nudge_5_off",
#                 "content":["popular_products"],
#                 "reason":"active but low engagement: a small nudge to increase activity"},
#     "experiment":{"id":"checkout_button_color","variant":"control"}}

curl localhost:8082/v1/experiments/checkout_button_color/results
```

### profile-consumer (`:8081`)

No public API beyond `/healthz`, `/readyz`, `/metrics` -- it's a
background Kafka consumer, not a request-driven service.

## Running it

Requires Docker + Docker Compose. Kafka runs in KRaft mode (no
Zookeeper) via the official `apache/kafka` image.

```bash
make up        # docker compose up -d --build
make logs      # tail every service
make ps        # container status
./scripts/smoke_test.sh   # end-to-end check: ingest -> profile -> strategy -> A/B convert -> results
make down      # tear down (drops the postgres volume too)
```

Default active experiment (`checkout_button_color`, 50/50 control vs.
treatment) is seeded by `migrations/001_init.sql` on first Postgres
boot.

Run tests and build locally (outside Docker) with:

```bash
make build     # go build ./...
make test      # go test ./... -race
```

## Load test results

Generated against the docker-compose stack running in a **2 vCPU**
sandbox with Kafka, Postgres, Redis, Prometheus and all three Go
services sharing those same 2 cores -- i.e. a deliberately
resource-starved environment, not a tuned benchmark rig. Numbers below
are what actually came back; nothing here is estimated.

k6 wasn't installable in the sandbox that produced these numbers (its
install pulls a large dependency tree that didn't fit the time budget),
so the published numbers were generated with
[`hey`](https://github.com/rakyll/hey) instead. **`loadtest/ingest.js`
is a k6 script that exercises the same endpoint with randomized
user IDs and event types across a ramping-VU profile** -- use it if you
have k6 installed (`make loadtest`); it's the more realistic test of
the two since it varies payloads and ramps load instead of hammering
one shape at max concurrency immediately.

### `POST /v1/events` (single event, fixed payload)

| Concurrency | Duration | Requests/sec | p50    | p95    | p99    | Errors |
|-------------|----------|---------------|--------|--------|--------|--------|
| 200         | 30s      | **3,329 req/s** | 52ms  | 127ms  | 190ms  | 0 / 100,078 |
| 400         | 20s      | **4,009 req/s** | 86ms  | 200ms  | 317ms  | 0 / 80,615  |

```
hey -z 30s -c 200 -m POST -T "application/json" -D event.json http://localhost:8080/v1/events
```

Throughput scaled from c=200 to c=400 (3,329 -> 4,009 req/s) but latency
grew faster than throughput did -- a classic sign of hitting a resource
ceiling (here, 2 shared vCPUs across 7 containers) rather than an
application-level bottleneck. Zero failed requests at either
concurrency: the ingestion API stayed correct under load, it just got
slower as the box saturated.

### `POST /v1/events/batch` (50 events/request)

| Concurrency | Duration | Requests/sec | Effective events/sec | p50    | p99    | Errors |
|-------------|----------|---------------|----------------------|--------|--------|--------|
| 100         | 20s      | 583.8 req/s   | **~29,190 events/sec** | 149ms | 240ms | 0 / 13,186 |

Batching amortizes per-request HTTP + Kafka-publish overhead across 50
events, and it shows: roughly **7x the effective event throughput** of
the single-event endpoint on identical hardware. This is the standard
answer to "how do you make an HTTP ingestion API high-throughput" --
batch at the edge, not just inside the Kafka client.

**What this would look like with dedicated resources:** the ingestion
API itself does almost no CPU work per request (validate, marshal,
hand to the Kafka writer); the bottleneck here is CPU contention with
Kafka/Postgres/Redis on the same 2 cores, plus `kafka-go`'s
`RequiredAcks: RequireOne` + 50ms `BatchTimeout` adding a real floor to
per-request latency. On dedicated hardware (or with `Async: true` /
`RequiredAcks: RequireNone` for a throughput-over-durability tradeoff),
single-event throughput in the tens of thousands of req/s is a
realistic expectation for this code path -- the architecture doesn't
change, only the constant.

## Observability

Every service exposes `/metrics` (Prometheus text format) and
`/healthz` / `/readyz`. `docker compose up` also brings up Prometheus
(`:9090`) pre-configured (`deploy/prometheus.yml`) to scrape all three.

Key metrics (see `internal/metrics/metrics.go` for the full list):

- `http_requests_total`, `http_request_duration_seconds` -- every HTTP
  route, labeled by service/route/method/status (route is chi's
  **matched pattern**, e.g. `/v1/strategy/{userID}`, not the raw path --
  otherwise 10,000 distinct user IDs would mean 10,000 Prometheus label
  values).
- `kafka_publish_total{outcome}`, `kafka_publish_duration_seconds` --
  ingestion-api's producer health.
- `events_consumed_total{event_type}`, `profile_update_duration_seconds{stage}`
  -- consumer throughput, split by the Postgres vs. Redis write stage so
  you can see which one is the bottleneck if either gets slow.
- `experiment_exposures_total`, `experiment_conversions_total` -- both
  labeled by experiment + variant, so a Grafana panel can chart
  conversion rate over time per arm without hitting Postgres.

Logging is structured JSON via the standard library's `log/slog` (no
third-party logging dependency), with every HTTP request tagged with a
`request_id` (propagated via the `X-Request-ID` header) so a single
request's log lines can be grepped out of `docker compose logs` even
under concurrent load.

## Known limitations (and what I'd do next)

Being upfront about these is more useful than hiding them -- they're
also exactly what I'd bring up if asked "what would you change" in an
interview:

- **Exactly-once vs. at-least-once**: a crash between committing to
  Postgres and committing the Kafka offset would reprocess (and
  double-count) that one event. Fixing this properly means either
  storing the last-processed Kafka offset *in the same Postgres
  transaction* as the profile update (true exactly-once), or making
  `ApplyEvent` idempotent per `event_id` (a `processed_events` dedup
  table). Left as at-least-once here because it's the honest default
  and the simpler design; the fix is well understood, not a mystery.
- **Single active experiment**: the strategy engine buckets into one
  configured experiment (`ACTIVE_EXPERIMENT_ID`) rather than supporting
  concurrent multi-experiment assignment with mutual-exclusion groups
  (a real experimentation platform -- Optimizely, GrowthBook -- needs
  this). The bucketing function itself (`experiment.Assign`) already
  supports arbitrary experiment IDs; what's missing is an admin API and
  a "which experiments is this user eligible for" layer on top.
- **No sequential-testing correction** on the z-test: peeking at
  `/results` repeatedly during a live experiment and stopping as soon as
  it looks significant inflates the false-positive rate (the "peeking
  problem"). A real platform uses always-valid sequential tests or a
  pre-registered sample size. Documented rather than solved because
  it's a legitimate rabbit hole, not an oversight.
- **Rule-based strategy engine, not ML**: `strategy.Decide` is an
  explicit, explainable rule table over lifecycle stage + engagement
  score. That's the right starting point (explainable, no cold-start
  problem, no training pipeline needed) -- the natural evolution is a
  learned ranking/propensity model reading the same profile features,
  with the rule engine kept as a fallback for new/cold-start users.
- **Kafka partition count is 1** (auto-created topic default). Fine for
  a demo; a real deployment would size partitions for target consumer
  parallelism up front, since repartitioning a live topic is disruptive.
- **No auth** on any endpoint -- out of scope for a portfolio
  architecture demo, but would obviously be required before this touched
  real traffic (mTLS or a shared secret between services at minimum,
  proper authn/authz on any endpoint reachable from outside the cluster).

## Repo layout

```
cmd/
  ingestion-api/      HTTP -> Kafka
  profile-consumer/   Kafka -> Postgres + Redis
  strategy-api/       personalization + A/B endpoints
internal/
  config/     env-driven configuration, shared by all three binaries
  events/     event schema + validation
  kafkax/     thin kafka-go wrapper (keyed producer, group consumer)
  profile/    scoring/lifecycle logic (pure, unit tested) + Postgres/Redis repos
  experiment/ deterministic bucketing + z-test (pure, unit tested) + Postgres store
  strategy/   rule-based personalization engine (pure, unit tested)
  httpmw/     request id, access log, recoverer, metrics middleware
  metrics/    Prometheus metric definitions
  logging/    slog JSON setup
migrations/   Postgres schema + seed data
deploy/       prometheus.yml
loadtest/     k6 script
scripts/      smoke_test.sh
```

## Extending this project

A few natural next milestones, roughly in order of how much they'd
teach:

1. Add a `processed_events` idempotency table and get real exactly-once
   semantics -- a genuinely hard distributed-systems problem to solve
   correctly.
2. Multi-experiment support with mutual exclusion groups in
   `internal/experiment`.
3. Swap the rule-based strategy engine for a logistic-regression
   propensity model trained on the same profile features (still
   servable with zero new infra -- just a different function inside
   `strategy.Decide`).
4. A Grafana dashboard on top of the existing Prometheus metrics
   (`docker-compose.yml` already exposes `:9090`; adding Grafana is one
   more service block).
5. Repartition `behavior.events` and run multiple `profile-consumer`
   replicas to see Kafka's consumer-group rebalancing do its job.
