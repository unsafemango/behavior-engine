package experiment

// Experiment describes one A/B test: its variants and their traffic
// weights. Kept in Postgres (see store.go) so new experiments can be
// added without a deploy, though this project ships with a single
// active experiment (ACTIVE_EXPERIMENT_ID) for the strategy engine to
// bucket users into.
type Experiment struct {
	ID       string
	Name     string
	Active   bool
	Variants []Variant
}

// VariantResult is one row of an experiment's results readout.
type VariantResult struct {
	Variant     string  `json:"variant"`
	Exposures   int64   `json:"exposures"`
	Conversions int64   `json:"conversions"`
	Rate        float64 `json:"conversion_rate"`
}

// Results is the full readout for GET /v1/experiments/{id}/results.
// Significance is computed against the first variant in Variants,
// treated as the control/baseline.
type Results struct {
	ExperimentID string          `json:"experiment_id"`
	Variants     []VariantResult `json:"variants"`
	Baseline     string          `json:"baseline_variant,omitempty"`
	VsBaseline   []ZTestSummary  `json:"vs_baseline,omitempty"`
}

// ZTestSummary attaches a variant name to its ZTestResult against the
// baseline, for the results endpoint.
type ZTestSummary struct {
	Variant string      `json:"variant"`
	Test    ZTestResult `json:"z_test"`
}
