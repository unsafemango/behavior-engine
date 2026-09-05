package profile

import (
	"testing"
	"time"

	"github.com/unsafemango/behavior-engine/internal/events"
)

func TestApplyEvent_AccumulatesCounts(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := Profile{}

	p = ApplyEvent(p, events.Event{UserID: "u1", Type: events.TypePageView, Timestamp: base})
	p = ApplyEvent(p, events.Event{UserID: "u1", Type: events.TypeClick, Timestamp: base.Add(time.Minute)})
	p = ApplyEvent(p, events.Event{UserID: "u1", Type: events.TypePurchase, Timestamp: base.Add(2 * time.Minute)})

	if p.TotalEvents != 3 {
		t.Errorf("expected 3 total events, got %d", p.TotalEvents)
	}
	if p.PageViews != 1 || p.Clicks != 1 || p.Purchases != 1 {
		t.Errorf("unexpected counts: %+v", p)
	}
	if !p.LastEventAt.Equal(base.Add(2 * time.Minute)) {
		t.Errorf("expected last event at to advance, got %v", p.LastEventAt)
	}
	if !p.FirstSeenAt.Equal(base) {
		t.Errorf("expected first seen at %v, got %v", base, p.FirstSeenAt)
	}
}

func TestApplyEvent_OutOfOrderEventsDontRewindLastEventAt(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := Profile{}
	p = ApplyEvent(p, events.Event{UserID: "u1", Type: events.TypeClick, Timestamp: base.Add(time.Hour)})
	p = ApplyEvent(p, events.Event{UserID: "u1", Type: events.TypeClick, Timestamp: base}) // arrives late, older

	if !p.LastEventAt.Equal(base.Add(time.Hour)) {
		t.Errorf("expected last_event_at to stay at the later timestamp, got %v", p.LastEventAt)
	}
	if !p.FirstSeenAt.Equal(base) {
		t.Errorf("expected first_seen_at to move earlier to %v, got %v", base, p.FirstSeenAt)
	}
}

func TestCurrentEngagementScore_DecaysWithAge(t *testing.T) {
	p := Profile{EngagementScore: 100, LastEventAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}

	fresh := p.CurrentEngagementScore(p.LastEventAt)
	if fresh != 100 {
		t.Errorf("expected no decay at age 0, got %v", fresh)
	}

	halfLifeLater := p.CurrentEngagementScore(p.LastEventAt.Add(engagementHalfLife))
	if halfLifeLater != 50 {
		t.Errorf("expected score to halve after one half-life, got %v", halfLifeLater)
	}

	twoHalfLivesLater := p.CurrentEngagementScore(p.LastEventAt.Add(2 * engagementHalfLife))
	if twoHalfLivesLater != 25 {
		t.Errorf("expected score to quarter after two half-lives, got %v", twoHalfLivesLater)
	}
}

func TestClassifyStage(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name        string
		totalEvents int64
		firstSeen   time.Time
		lastEvent   time.Time
		want        LifecycleStage
	}{
		{"brand new user", 1, now, now, StageNew},
		{"new but already active-recency, still under event floor", 3, now.Add(-time.Hour), now, StageNew},
		{"active", 20, now.Add(-30 * 24 * time.Hour), now.Add(-time.Hour), StageActive},
		{"at risk", 20, now.Add(-30 * 24 * time.Hour), now.Add(-5 * 24 * time.Hour), StageAtRisk},
		{"churned", 20, now.Add(-90 * 24 * time.Hour), now.Add(-40 * 24 * time.Hour), StageChurned},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ClassifyStage(c.totalEvents, c.firstSeen, c.lastEvent, now)
			if got != c.want {
				t.Errorf("ClassifyStage() = %v, want %v", got, c.want)
			}
		})
	}
}
