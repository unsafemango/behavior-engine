// Package experiment implements the A/B testing layer: deterministic
// bucketing, exposure/conversion tracking, and result aggregation with a
// basic significance test.
package experiment

import (
	"crypto/sha256"
	"encoding/binary"
)

// Variant is one arm of an experiment with its relative traffic weight.
// Weights don't need to sum to 100; Assign normalizes against the total.
type Variant struct {
	Name   string
	Weight int
}

// Assign deterministically buckets userID into one of variants for
// experimentID. Same (experimentID, userID) always yields the same
// variant -- no state, no storage lookup required -- which is what lets
// a user see a consistent experience across requests/services without
// a shared session store. Uses SHA-256 rather than a fast non-crypto
// hash so bucket assignment isn't trivially predictable/gameable, and
// because the throughput bottleneck here is Kafka/Postgres, not hashing.
//
// Returns ("", false) if variants is empty or all weights are <= 0.
func Assign(experimentID, userID string, variants []Variant) (string, bool) {
	total := 0
	for _, v := range variants {
		if v.Weight > 0 {
			total += v.Weight
		}
	}
	if total == 0 {
		return "", false
	}

	bucket := bucketOf(experimentID, userID, total)

	cumulative := 0
	for _, v := range variants {
		if v.Weight <= 0 {
			continue
		}
		cumulative += v.Weight
		if bucket < cumulative {
			return v.Name, true
		}
	}
	// Unreachable given the invariant above, but fall back to the last
	// positive-weight variant rather than panicking on a rounding edge.
	for i := len(variants) - 1; i >= 0; i-- {
		if variants[i].Weight > 0 {
			return variants[i].Name, true
		}
	}
	return "", false
}

// bucketOf hashes experimentID+":"+userID into a stable integer in
// [0, total).
func bucketOf(experimentID, userID string, total int) int {
	h := sha256.Sum256([]byte(experimentID + ":" + userID))
	n := binary.BigEndian.Uint64(h[:8])
	return int(n % uint64(total))
}
