// queue_test.go exercises persistence-port timing and outcome observations without changing queue
// behavior in the outbound adapter layer.

package metrics

import (
	"context"
	"errors"
	shared "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"net/http/httptest"
	"strings"
	"testing"
)

// queueFake injects queue outcomes while recording calls for metrics decorator assertions.
type queueFake struct {
	outbound.Queue
	err   error
	calls int
}

// Complete records the proposed result and returns the configured completion outcome, including
// ambiguity or lease loss.
func (q *queueFake) Complete(context.Context, outbound.Item, domain.Document) error {
	q.calls++
	return q.err
}

// TestPersistenceOutcomeDoesNotChangeCommitSemantics checks the timing decorator preserves the
// exact wrapped completion result, including ambiguity and lease loss.
func TestPersistenceOutcomeDoesNotChangeCommitSemantics(t *testing.T) {
	for _, tc := range []struct {
		err     error
		outcome string
	}{{nil, "completed"}, {outbound.ErrCommitUnknown, "unknown"}, {outbound.ErrLeaseLost, "interrupted"}} {
		base := &queueFake{err: tc.err}
		registry := shared.New("persist_block")
		q := Queue{Queue: base, Registry: registry}
		if err := q.Complete(t.Context(), outbound.Item{}, domain.Document{}); !errors.Is(err, tc.err) || base.calls != 1 {
			t.Fatal("persistence behavior changed")
		}
		output := httptest.NewRecorder()
		registry.Handler(func(context.Context) (shared.Snapshot, error) { return shared.Snapshot{}, nil }).ServeHTTP(output, httptest.NewRequest("GET", "/metrics", nil))
		if !strings.Contains(output.Body.String(), `etl_operations_total{operation="persist_block",outcome="`+tc.outcome+`"} 1`) {
			t.Fatal(output.Body.String())
		}
	}
}
