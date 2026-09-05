// Package profile owns the user profile model: the pure scoring and
// lifecycle-classification logic (unit tested, no I/O), plus Postgres
// (durable, source of truth) and Redis (fast lookup cache) repositories
// that the consumer writes to and the strategy engine reads from.
package profile

import (
	"math"
	"time"

	"github.com/unsafemango/behavior-engine/internal/events"
)

// LifecycleStage buckets a user by recency/tenure so the strategy engine
// and any downstream analytics can branch on a small, stable enum
// instead of raw timestamps.
type LifecycleStage string

const (
	StageNew     LifecycleStage = "new"
	StageActive  LifecycleStage = "active"
	StageAtRisk  LifecycleStage = "at_risk"
	StageChurned LifecycleStage = "churned"
)

// engagementHalfLife controls how fast a user's engagement score decays
// once they go quiet: every halfLife with no new activity, the *current*
// (as-read) score is worth half what it was. See CurrentEngagementScore.
const engagementHalfLife = 14 * 24 * time.Hour

// Profile is the aggregate behavior-engine maintains per user. Postgres
// is the durable copy; Redis holds the same shape (JSON-encoded) for
// sub-millisecond reads from the strategy API.
//
// EngagementScore is intentionally the *raw*, undecayed weighted score
// as of LastEventAt -- not a value that would go stale the moment time
// passes with no new events. Decay is applied lazily at read time via
// CurrentEngagementScore(now), so nothing needs to re-scan or re-write
// idle users' rows just because the clock moved.
type Profile struct {
	UserID           string         `json:"user_id"`
	FirstSeenAt      time.Time      `json:"first_seen_at"`
	LastEventAt      time.Time      `json:"last_event_at"`
	TotalEvents      int64          `json:"total_events"`
	PageViews        int64          `json:"page_views"`
	Clicks           int64          `json:"clicks"`
	Purchases        int64          `json:"purchases"`
	LifecycleEvents  int64          `json:"lifecycle_events"`
	EngagementScore  float64        `json:"engagement_score"` // raw, as of last_event_at
	LifecycleStage   LifecycleStage `json:"lifecycle_stage"`
	RecentEventTypes []string       `json:"recent_event_types,omitempty"` // most-recent-first, capped
	UpdatedAt        time.Time      `json:"updated_at"`
}

// Scoring weights. Purchases count far more than a page view because
// they're the strongest engagement signal; these are intentionally
// simple constants rather than a learned model -- see README for how
// this would evolve into something ML-driven.
const (
	weightPageView  = 0.5
	weightClick     = 1.0
	weightPurchase  = 5.0
	weightLifecycle = 2.0
)

// rawEngagementScore computes the undecayed weighted sum of activity.
func rawEngagementScore(pageViews, clicks, purchases, lifecycleEvents int64) float64 {
	sum := float64(pageViews)*weightPageView +
		float64(clicks)*weightClick +
		float64(purchases)*weightPurchase +
		float64(lifecycleEvents)*weightLifecycle
	return round2(sum)
}

// CurrentEngagementScore applies recency decay to the stored raw score,
// relative to now. A user with a high raw score who's gone quiet for a
// month will score much lower here than one active today with the same
// raw score -- this is what the strategy engine should call, not the
// stored EngagementScore field directly.
func (p Profile) CurrentEngagementScore(now time.Time) float64 {
	if p.EngagementScore == 0 {
		return 0
	}
	age := now.Sub(p.LastEventAt)
	if age < 0 {
		age = 0
	}
	return round2(p.EngagementScore * decayFactor(age, engagementHalfLife))
}

// decayFactor implements exponential decay: 0.5^(age/halfLife).
func decayFactor(age, halfLife time.Duration) float64 {
	if halfLife <= 0 {
		return 1
	}
	exponent := float64(age) / float64(halfLife)
	return math.Pow(0.5, exponent)
}

// round2 rounds to 2 decimal places for stable, human-readable scores.
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// ClassifyStage derives a LifecycleStage from tenure and recency,
// evaluated relative to now so it's pure and trivially unit-testable.
//
// Rules (documented in README):
//   - new:     first seen within 7 days AND fewer than 5 total events
//   - active:  last event within 3 days
//   - at_risk: last event between 3 and 14 days ago
//   - churned: last event more than 14 days ago
func ClassifyStage(totalEvents int64, firstSeenAt, lastEventAt, now time.Time) LifecycleStage {
	sinceLast := now.Sub(lastEventAt)
	sinceFirst := now.Sub(firstSeenAt)

	if sinceFirst <= 7*24*time.Hour && totalEvents < 5 {
		return StageNew
	}
	switch {
	case sinceLast <= 3*24*time.Hour:
		return StageActive
	case sinceLast <= 14*24*time.Hour:
		return StageAtRisk
	default:
		return StageChurned
	}
}

// ApplyEvent folds one event into an existing (possibly zero-value)
// profile, returning the updated profile. Pure function: no I/O, so the
// consumer and its tests can both use it directly. Lifecycle stage is
// evaluated as of the event's own timestamp (i.e. "what stage did this
// put them in at the time"); the strategy engine re-derives the
// *current* stage at read time the same way it re-derives engagement,
// via ClassifyStage(..., time.Now()).
func ApplyEvent(p Profile, e events.Event) Profile {
	if p.FirstSeenAt.IsZero() || e.Timestamp.Before(p.FirstSeenAt) {
		p.FirstSeenAt = e.Timestamp
	}
	if p.UserID == "" {
		p.UserID = e.UserID
	}
	if e.Timestamp.After(p.LastEventAt) {
		p.LastEventAt = e.Timestamp
	}

	p.TotalEvents++
	switch e.Type {
	case events.TypePageView:
		p.PageViews++
	case events.TypeClick:
		p.Clicks++
	case events.TypePurchase:
		p.Purchases++
	case events.TypeLifecycle:
		p.LifecycleEvents++
	}

	p.RecentEventTypes = pushRecent(p.RecentEventTypes, string(e.Type), 10)
	p.EngagementScore = rawEngagementScore(p.PageViews, p.Clicks, p.Purchases, p.LifecycleEvents)
	p.LifecycleStage = ClassifyStage(p.TotalEvents, p.FirstSeenAt, p.LastEventAt, p.LastEventAt)
	p.UpdatedAt = e.Timestamp
	return p
}

// pushRecent prepends v to list, capping its length at max.
func pushRecent(list []string, v string, max int) []string {
	list = append([]string{v}, list...)
	if len(list) > max {
		list = list[:max]
	}
	return list
}
