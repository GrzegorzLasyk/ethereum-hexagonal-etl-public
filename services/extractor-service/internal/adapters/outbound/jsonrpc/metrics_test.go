// metrics_test.go exercises low-cardinality operation/queue exposition with snapshot
// failure/freshness semantics in the outbound adapter layer.

package jsonrpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
)

// TestRPCMetricsCountActualRetriesAndBudget compares transport reservations with metrics so
// retries consume the same actual-attempt budget.
func TestRPCMetricsCountActualRetriesAndBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig("http://127.0.0.1:1/private-secret")
		cfg.MaxRequests = 3
		cfg.MaxAttempts = 3
		registry := metrics.New()
		calls := 0
		c, err := New(cfg, &http.Client{Transport: budgetTransport(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}, registry)
		if err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, _ = c.ChainID(canceled)
		for range 2 {
			if _, err = c.ChainID(t.Context()); !errors.Is(err, outbound.ErrRPCBudgetExceeded) {
				t.Fatal(err)
			}
		}
		if calls != 3 || c.Requests() != 3 {
			t.Fatal("changed request accounting")
		}
		w := httptest.NewRecorder()
		registry.Handler(func(context.Context) (metrics.Snapshot, error) {
			return metrics.Snapshot{}, errors.New("private-db-error")
		}).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		body := w.Body.String()
		for _, want := range []string{"etl_rpc_attempts_total 3\n", "etl_rpc_retries_total 2\n", "etl_rpc_budget_remaining 0\n", "etl_database_snapshot_success 0\n"} {
			if !strings.Contains(body, want) {
				t.Fatal("missing RPC evidence", want)
			}
		}
		if strings.Contains(body, "private") {
			t.Fatal("secret leaked")
		}
	})
}
