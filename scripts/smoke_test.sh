#!/usr/bin/env bash
# End-to-end smoke test against a running docker-compose stack:
# ingest events for one user -> wait for the consumer to catch up ->
# read the built profile -> read a strategy decision -> exercise the
# A/B endpoints. Exits non-zero on the first failed check.
set -euo pipefail

INGEST_URL="${INGEST_URL:-http://localhost:8080}"
STRATEGY_URL="${STRATEGY_URL:-http://localhost:8082}"
USER_ID="smoke-test-user-$RANDOM"
EXPERIMENT_ID="checkout_button_color"

fail() { echo "FAIL: $1" >&2; exit 1; }
pass() { echo "OK:   $1"; }

echo "== behavior-engine smoke test (user=$USER_ID) =="

for i in 1 2 3 4 5; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST_URL/v1/events" \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$USER_ID\",\"type\":\"page_view\"}")
  [ "$code" = "202" ] || fail "event $i ingest returned $code"
done
pass "ingested 5 page_view events"

echo "waiting for profile-consumer to catch up..."
sleep 3

profile=$(curl -s "$STRATEGY_URL/v1/profiles/$USER_ID")
echo "$profile" | grep -q "\"total_events\":5" || fail "profile does not show 5 total_events: $profile"
pass "profile shows 5 total_events"

strategy=$(curl -s "$STRATEGY_URL/v1/strategy/$USER_ID")
echo "$strategy" | grep -q "ui_variant" || fail "strategy response missing ui_variant: $strategy"
echo "$strategy" | grep -q "\"experiment\"" || fail "strategy response missing experiment assignment: $strategy"
pass "strategy decision returned with experiment assignment"

convert_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$STRATEGY_URL/v1/experiments/$EXPERIMENT_ID/convert" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$USER_ID\"}")
[ "$convert_code" = "200" ] || fail "conversion returned $convert_code"
pass "conversion recorded"

results=$(curl -s "$STRATEGY_URL/v1/experiments/$EXPERIMENT_ID/results")
echo "$results" | grep -q "variants" || fail "results missing variants: $results"
pass "results endpoint returned variant breakdown"

echo "== all checks passed =="
