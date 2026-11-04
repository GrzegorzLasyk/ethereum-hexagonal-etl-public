// stats.go implements bounded duration sampling and reproducible baseline summaries in the shared
// technical support layer, outside service domains.

package baseline

import (
	"math"
	"slices"
)

// Distribution retains a bounded exact sample. Quantiles are withheld if capped,
// rather than presenting the first samples as an unbiased population estimate.
type Distribution struct {
	Count      uint64    `json:"count"`
	SumSeconds float64   `json:"sum_seconds"`
	P50        *float64  `json:"p50_seconds"`
	P95        *float64  `json:"p95_seconds"`
	Samples    []float64 `json:"-"`
}

// SampleLimit caps retained latency samples while aggregate counts remain available.
const SampleLimit = 100000

// Add adds a duration to bounded samples while retaining aggregate count/total information.
func (d *Distribution) Add(seconds float64) {
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return
	}
	d.Count++
	d.SumSeconds += seconds
	if len(d.Samples) < SampleLimit {
		d.Samples = append(d.Samples, seconds)
	}
}

// Summary sorts a copy of retained samples to report quantiles without mutating observation order.
func (d Distribution) Summary() Distribution {
	d.P50, d.P95 = nil, nil
	if d.Count > 0 && d.Count == uint64(len(d.Samples)) {
		values := slices.Clone(d.Samples)
		slices.Sort(values)
		a, b := values[int(math.Ceil(float64(len(values))*.5))-1], values[int(math.Ceil(float64(len(values))*.95))-1]
		d.P50, d.P95 = &a, &b
	}
	d.Samples = nil
	return d
}
