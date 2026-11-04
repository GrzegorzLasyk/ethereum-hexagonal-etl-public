// metrics_test.go exercises low-cardinality operation/queue exposition with snapshot
// failure/freshness semantics in the shared technical support layer, outside service domains.

package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// scrape serves the metrics handler through a controlled HTTP recorder and returns its exposition
// for assertions.
func scrape(h http.Handler) string {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	return w.Body.String()
}

// TestSnapshotFailureFreshnessAndRecovery checks database observation failure remains explicit and
// later successful snapshots restore valid queue data.
func TestSnapshotFailureFreshnessAndRecovery(t *testing.T) {
	r := New("process_inbox")
	r.Observe("process_inbox", "retry", time.Second)
	fail := false
	cp := uint64(42)
	r.Head(45, time.Now())
	h := r.Handler(func(ctx context.Context) (Snapshot, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("missing deadline")
		}
		if fail {
			return Snapshot{}, errors.New("postgres://secret")
		}
		return Snapshot{Queues: []Queue{{Kind: "inbox", Pending: 2, Failed: 1, OldestAge: 10}}, Extractor: true, Checkpoint: &cp, Revision: 3}, nil
	})
	good := scrape(h)
	for _, s := range []string{"etl_database_snapshot_success 1", "etl_queue_backlog{queue=\"inbox\"} 3", "etl_extractor_lag_blocks 3", "etl_extractor_revision 3"} {
		if !strings.Contains(good, s) {
			t.Fatal(s, good)
		}
	}
	fail = true
	bad := scrape(h)
	for _, s := range []string{"etl_queue_", "etl_extractor_checkpoint", "etl_extractor_revision", "etl_extractor_lag_blocks", "secret"} {
		if strings.Contains(bad, s) {
			t.Fatal("false or sensitive sample", s)
		}
	}
	if !strings.Contains(bad, "etl_database_snapshot_success 0") || !strings.Contains(bad, "etl_operation_retries_total{operation=\"process_inbox\"} 1") {
		t.Fatal(bad)
	}
	fail = false
	if !strings.Contains(scrape(h), "etl_database_snapshot_success 1") {
		t.Fatal("did not recover")
	}
}

// TestBoundedLabelsHistogramsConcurrencyAndRestart checks concurrent metric updates, fixed label
// vocabulary and process-local counter reset semantics.
func TestBoundedLabelsHistogramsConcurrencyAndRestart(t *testing.T) {
	r := New("persist_block", "secret-event-id")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				r.Observe("persist_block", "completed", time.Millisecond)
				r.Observe("secret-event-id", "completed", time.Second)
				r.Observe("persist_block", "secret-error", time.Second)
			}
		})
	}
	h := r.Handler(func(context.Context) (Snapshot, error) { return Snapshot{}, nil })
	for range 10 {
		_ = scrape(h)
	}
	wg.Wait()
	body := scrape(h)
	for _, s := range []string{"etl_operations_total{operation=\"persist_block\",outcome=\"completed\"} 800", "etl_operation_duration_seconds_bucket{operation=\"persist_block\",le=\"+Inf\"} 800", "etl_operation_duration_seconds_bucket{operation=\"persist_block\",le=\"0.001\"} 800"} {
		if !strings.Contains(body, s) {
			t.Fatal(s)
		}
	}
	if strings.Contains(body, "secret") || strings.Contains(body, "etl_extractor_head ") {
		t.Fatal("unbounded labels or invented head")
	}
	fresh := New("persist_block")
	if !strings.Contains(scrape(fresh.Handler(func(context.Context) (Snapshot, error) { return Snapshot{}, nil })), "etl_operation_duration_seconds_count{operation=\"persist_block\"} 0") {
		t.Fatal("counter restart")
	}
}

// TestScrapeDeadlineAndConcurrencyBound checks DB-backed metric collection is time/concurrency
// bounded and cannot become unbounded work under scrape load.
func TestScrapeDeadlineAndConcurrencyBound(t *testing.T) {
	r := New()
	entered := make(chan struct{})
	done := make(chan string)
	h := r.Handler(func(ctx context.Context) (Snapshot, error) {
		close(entered)
		<-ctx.Done()
		return Snapshot{}, ctx.Err()
	})
	go func() { done <- scrape(h) }()
	<-entered
	if !strings.Contains(scrape(h), "etl_database_snapshot_success 0") {
		t.Fatal("busy scrape claimed success")
	}
	select {
	case body := <-done:
		if !strings.Contains(body, "etl_database_snapshot_success 0") {
			t.Fatal(body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unbounded scrape")
	}
}
