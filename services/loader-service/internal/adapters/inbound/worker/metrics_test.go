// metrics_test.go exercises low-cardinality operation/queue exposition with snapshot
// failure/freshness semantics in the inbound adapter layer.

package worker_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
)

// TestMetricsIndependentOfLogLevelAndShutdown checks worker observations remain available
// regardless of filtered logs or cancellation.
func TestMetricsIndependentOfLogLevelAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	registry := metrics.New("process_inbox")
	worker, err := worker.New(processorFunc(func(context.Context) (inbound.ProcessResult, error) {
		cancel()
		return inbound.ProcessResult{Found: true, Outcome: "retry", EventID: "unbounded-secret-id", Attempt: 2}, nil
	}), slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})), 1, time.Second, time.Second, registry)
	if err != nil {
		t.Fatal(err)
	}
	worker.Run(ctx)
	output := httptest.NewRecorder()
	registry.Handler(func(context.Context) (metrics.Snapshot, error) { return metrics.Snapshot{}, nil }).ServeHTTP(output, httptest.NewRequest("GET", "/metrics", nil))
	body := output.Body.String()
	if !strings.Contains(body, `etl_operation_retries_total{operation="process_inbox"} 1`) || !strings.Contains(body, `etl_operation_duration_seconds_count{operation="process_inbox"} 1`) || strings.Contains(body, "unbounded-secret-id") {
		t.Fatal(body)
	}
}
