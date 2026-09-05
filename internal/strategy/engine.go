// Package strategy turns a user profile into a personalization
// decision: which UI variant, promo and content to show. It's a
// rule-based scoring model on purpose -- simple, explainable, and fast
// -- with the extension point for a real ML model called out in the
// README rather than implemented (a proper ranking model needs
// training data behavior-engine doesn't generate).
package strategy

import (
	"time"

	"github.com/unsafemango/behavior-engine/internal/profile"
)

// Decision is what the strategy endpoint returns for a user.
type Decision struct {
	UIVariant string   `json:"ui_variant"`
	Promo     string   `json:"promo,omitempty"`
	Content   []string `json:"content"`
	Reason    string   `json:"reason"`
}

// Decide applies the rule set to p as of now. now is threaded through
// explicitly (rather than calling time.Now() internally) so this stays
// a pure, trivially unit-testable function.
func Decide(p profile.Profile, now time.Time) Decision {
	score := p.CurrentEngagementScore(now)
	stage := profile.ClassifyStage(p.TotalEvents, p.FirstSeenAt, p.LastEventAt, now)

	switch stage {
	case profile.StageNew:
		return Decision{
			UIVariant: "onboarding",
			Promo:     "welcome_10_off",
			Content:   []string{"getting_started_guide", "popular_products"},
			Reason:    "new user: prioritize onboarding and a first-purchase incentive",
		}
	case profile.StageAtRisk:
		return Decision{
			UIVariant: "winback",
			Promo:     "comeback_15_off",
			Content:   []string{"whats_new", "recommended_for_you"},
			Reason:    "at-risk user: re-engagement promo before they churn",
		}
	case profile.StageChurned:
		return Decision{
			UIVariant: "reactivation",
			Promo:     "reactivate_20_off",
			Content:   []string{"whats_new"},
			Reason:    "churned user: strongest incentive to bring them back",
		}
	}

	// Active users are further split by engagement score, since "active"
	// spans everything from "clicked once yesterday" to "power user".
	switch {
	case score >= 20:
		return Decision{
			UIVariant: "power_user",
			Content:   []string{"advanced_features", "loyalty_program"},
			Reason:    "high engagement score: surface advanced features and loyalty perks instead of a discount",
		}
	case score >= 5:
		return Decision{
			UIVariant: "standard",
			Content:   []string{"recommended_for_you", "trending"},
			Reason:    "moderate engagement: standard personalized content, no promo needed",
		}
	default:
		return Decision{
			UIVariant: "standard",
			Promo:     "small_nudge_5_off",
			Content:   []string{"popular_products"},
			Reason:    "active but low engagement: a small nudge to increase activity",
		}
	}
}
