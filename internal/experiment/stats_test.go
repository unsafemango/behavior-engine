package experiment

import "testing"

func TestTwoProportionZTest_ClearWinner(t *testing.T) {
	// Baseline: 100/1000 = 10%. Variant: 150/1000 = 15%. Large sample,
	// clear lift -- should read as significant.
	res := TwoProportionZTest(100, 1000, 150, 1000)
	if !res.SignificantAt95 {
		t.Errorf("expected a clear 10%%->15%% lift on n=1000 each to be significant, got p=%v z=%v", res.PValue, res.ZScore)
	}
	if res.RelativeLiftPercent <= 0 {
		t.Errorf("expected positive relative lift, got %v", res.RelativeLiftPercent)
	}
}

func TestTwoProportionZTest_NoDifference(t *testing.T) {
	res := TwoProportionZTest(100, 1000, 101, 1000)
	if res.SignificantAt95 {
		t.Errorf("expected a near-identical rate to not be significant, got p=%v", res.PValue)
	}
}

func TestTwoProportionZTest_ZeroExposuresIsSafe(t *testing.T) {
	res := TwoProportionZTest(0, 0, 5, 100)
	if res.SignificantAt95 || res.ZScore != 0 {
		t.Errorf("expected zero-value result for zero baseline exposures, got %+v", res)
	}
}
