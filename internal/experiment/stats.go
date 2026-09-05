package experiment

import "math"

// ZTestResult is a two-proportion z-test comparing a variant's
// conversion rate against a baseline (control) variant.
type ZTestResult struct {
	ZScore              float64
	PValue              float64
	SignificantAt95     bool
	SignificantAt99     bool
	BaselineRate        float64
	VariantRate         float64
	RelativeLiftPercent float64
}

// TwoProportionZTest runs a standard two-proportion z-test. It's
// deliberately simple (no sequential-testing correction, no Bayesian
// credible intervals) -- appropriate for a fixed-horizon readout, and
// documented as a known limitation in the README rather than pretending
// to be a full experimentation platform.
func TwoProportionZTest(baselineConversions, baselineExposures, variantConversions, variantExposures int64) ZTestResult {
	if baselineExposures == 0 || variantExposures == 0 {
		return ZTestResult{}
	}

	p1 := float64(baselineConversions) / float64(baselineExposures)
	p2 := float64(variantConversions) / float64(variantExposures)
	n1 := float64(baselineExposures)
	n2 := float64(variantExposures)

	pooled := float64(baselineConversions+variantConversions) / (n1 + n2)
	se := math.Sqrt(pooled * (1 - pooled) * (1/n1 + 1/n2))

	var z float64
	if se > 0 {
		z = (p2 - p1) / se
	}

	pValue := twoSidedPValue(z)

	lift := 0.0
	if p1 > 0 {
		lift = round2((p2 - p1) / p1 * 100)
	}

	return ZTestResult{
		ZScore:              round2(z),
		PValue:              round4(pValue),
		SignificantAt95:     pValue < 0.05,
		SignificantAt99:     pValue < 0.01,
		BaselineRate:        round4(p1),
		VariantRate:         round4(p2),
		RelativeLiftPercent: lift,
	}
}

// twoSidedPValue converts a z-score to a two-sided p-value using the
// standard normal CDF (via math.Erf, in the standard library -- no
// stats dependency needed for this one calculation).
func twoSidedPValue(z float64) float64 {
	if z < 0 {
		z = -z
	}
	// 1 - CDF(z), doubled for two-sided.
	return 2 * (1 - normalCDF(z))
}

func normalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round4(v float64) float64 { return math.Round(v*10000) / 10000 }
