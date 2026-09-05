package strategy

import (
	"testing"
	"time"

	"github.com/unsafemango/behavior-engine/internal/profile"
)

func TestDecide_NewUserGetsOnboarding(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := profile.Profile{TotalEvents: 1, FirstSeenAt: now, LastEventAt: now}

	d := Decide(p, now)
	if d.UIVariant != "onboarding" {
		t.Errorf("expected onboarding variant for new user, got %q", d.UIVariant)
	}
	if d.Promo == "" {
		t.Error("expected a welcome promo for new users")
	}
}

func TestDecide_ChurnedUserGetsReactivation(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := profile.Profile{
		TotalEvents:  50,
		FirstSeenAt:  now.Add(-120 * 24 * time.Hour),
		LastEventAt:  now.Add(-40 * 24 * time.Hour),
	}

	d := Decide(p, now)
	if d.UIVariant != "reactivation" {
		t.Errorf("expected reactivation variant for churned user, got %q", d.UIVariant)
	}
}

func TestDecide_HighEngagementActiveUserSkipsPromo(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := profile.Profile{
		TotalEvents:     200,
		FirstSeenAt:     now.Add(-120 * 24 * time.Hour),
		LastEventAt:     now.Add(-time.Hour),
		EngagementScore: 500, // high raw score, minimal decay at 1 hour old
	}

	d := Decide(p, now)
	if d.UIVariant != "power_user" {
		t.Errorf("expected power_user variant, got %q", d.UIVariant)
	}
	if d.Promo != "" {
		t.Errorf("expected no promo for high-engagement active user, got %q", d.Promo)
	}
}

func TestDecide_LowEngagementActiveUserGetsNudge(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := profile.Profile{
		TotalEvents:     10,
		FirstSeenAt:     now.Add(-30 * 24 * time.Hour),
		LastEventAt:     now.Add(-time.Hour),
		EngagementScore: 2,
	}

	d := Decide(p, now)
	if d.UIVariant != "standard" || d.Promo == "" {
		t.Errorf("expected standard variant with a small nudge promo, got %+v", d)
	}
}
