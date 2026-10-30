package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdmissionMetricsAndFailedSnapshot(t *testing.T) {
	r := New()
	r.ConfigureAdmission(2, 100, 200)
	r.AdmissionActive(1)
	r.AdmissionRejected("transport")
	r.AdmissionRejected("durable")
	r.AdmissionRejected("unbounded-secret")
	r.OverloadRetry()
	check := func(fail bool) string {
		h := r.Handler(func(context.Context) (Snapshot, error) {
			if fail {
				return Snapshot{}, errors.New("database secret")
			}
			return Snapshot{AdmissionUsed: 4, AdmissionLimit: 4}, nil
		})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		return w.Body.String()
	}
	good := check(false)
	for _, want := range []string{"etl_admission_inflight_requests 1", "etl_admission_reserved_bytes 100", "etl_admission_capacity_utilization 1", "etl_admission_durable_backpressure 1", "etl_overload_retries_total 1", "etl_admission_rejections_total{reason=\"durable\"} 1"} {
		if !strings.Contains(good, want) {
			t.Fatal("missing metric", want)
		}
	}
	bad := check(true)
	if strings.Contains(bad, "etl_admission_capacity_utilization") || strings.Contains(bad, "secret") || !strings.Contains(bad, "etl_database_snapshot_success 0") {
		t.Fatal("failed database scrape fabricated capacity")
	}
	r.AdmissionActive(-1)
	if !strings.Contains(check(false), "etl_admission_inflight_requests 0") {
		t.Fatal("inflight leak")
	}
}
