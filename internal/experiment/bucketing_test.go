package experiment

import (
	"fmt"
	"math"
	"testing"
)

func TestAssign_Deterministic(t *testing.T) {
	variants := []Variant{{"control", 50}, {"treatment", 50}}
	v1, ok1 := Assign("exp1", "user-42", variants)
	v2, ok2 := Assign("exp1", "user-42", variants)
	if !ok1 || !ok2 {
		t.Fatal("expected assignment to succeed")
	}
	if v1 != v2 {
		t.Errorf("expected same user to always get same variant, got %q then %q", v1, v2)
	}
}

func TestAssign_DifferentExperimentsCanDifferForSameUser(t *testing.T) {
	variants := []Variant{{"control", 50}, {"treatment", 50}}
	// Not asserting they *must* differ (that'd be flaky), just that both
	// resolve to a valid variant independently.
	v1, ok1 := Assign("exp1", "user-42", variants)
	v2, ok2 := Assign("exp2", "user-42", variants)
	if !ok1 || !ok2 {
		t.Fatal("expected both assignments to succeed")
	}
	if v1 == "" || v2 == "" {
		t.Error("expected non-empty variant names")
	}
}

func TestAssign_NoPositiveWeights(t *testing.T) {
	if _, ok := Assign("exp1", "user-1", []Variant{{"a", 0}, {"b", -5}}); ok {
		t.Error("expected assignment to fail when no variant has positive weight")
	}
	if _, ok := Assign("exp1", "user-1", nil); ok {
		t.Error("expected assignment to fail on empty variant list")
	}
}

func TestAssign_RoughlyRespectsWeights(t *testing.T) {
	variants := []Variant{{"control", 90}, {"treatment", 10}}
	const n = 20000
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		userID := fmt.Sprintf("user-%d", i)
		v, ok := Assign("weighted-exp", userID, variants)
		if !ok {
			t.Fatalf("assignment failed for %s", userID)
		}
		counts[v]++
	}

	gotTreatmentPct := float64(counts["treatment"]) / float64(n) * 100
	if math.Abs(gotTreatmentPct-10) > 1.5 { // allow +/-1.5pp slack across 20k samples
		t.Errorf("expected ~10%% of users in treatment, got %.2f%% (counts=%v)", gotTreatmentPct, counts)
	}
}
