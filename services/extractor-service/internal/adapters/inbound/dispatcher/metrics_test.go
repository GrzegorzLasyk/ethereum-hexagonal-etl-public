package dispatcher_test

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/dispatcher"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsIndependentOfLogLevelAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	registry := metrics.New("dispatch_outbox")
	worker, err := dispatcher.New(processorFunc(func(context.Context) (inbound.DispatchResult, error) {
		cancel()
		return inbound.DispatchResult{Found: true, Outcome: "retry", EventID: "unbounded-secret-id", Attempt: 2}, nil
	}), slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})), 1, time.Second, time.Second, registry)
	if err != nil {
		t.Fatal(err)
	}
	worker.Run(ctx)
	output := httptest.NewRecorder()
	registry.Handler(func(context.Context) (metrics.Snapshot, error) { return metrics.Snapshot{}, nil }).ServeHTTP(output, httptest.NewRequest("GET", "/metrics", nil))
	body := output.Body.String()
	if !strings.Contains(body, `etl_operation_retries_total{operation="dispatch_outbox"} 1`) || !strings.Contains(body, `etl_operation_duration_seconds_count{operation="dispatch_outbox"} 1`) || strings.Contains(body, "unbounded-secret-id") {
		t.Fatal(body)
	}
}
