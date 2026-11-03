package baseline

import "testing"

func TestDistribution(t *testing.T) {
	var d Distribution
	if d.Summary().P95 != nil {
		t.Fatal("empty quantile")
	}
	for i := 100; i > 0; i-- {
		d.Add(float64(i))
	}
	s := d.Summary()
	if s.Count != 100 || *s.P50 != 50 || *s.P95 != 95 || s.SumSeconds != 5050 {
		t.Fatal(s)
	}
	d.Count = SampleLimit + 1
	if d.Summary().P50 != nil {
		t.Fatal("biased capped quantile")
	}
}
