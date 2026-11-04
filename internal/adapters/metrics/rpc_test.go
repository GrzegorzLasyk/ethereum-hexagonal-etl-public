// rpc_test.go exercises actual RPC attempt/retry/budget metrics with process restart semantics in
// the shared technical support layer, outside service domains.

package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// TestRPCBudgetExposition checks finite/unlimited attempt metrics and process restart/reset
// semantics without high-cardinality labels.
func TestRPCBudgetExposition(t *testing.T) {
	r := New()
	var b bytes.Buffer
	r.writeRPC(&b)
	if b.Len() != 0 {
		t.Fatal("RPC not configured")
	}
	r.ConfigureRPC(8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() { r.RPCAttempt(i%2 == 1) })
	}
	wg.Wait()
	r.writeRPC(&b)
	for _, want := range []string{"etl_rpc_attempts_total 8\n", "etl_rpc_retries_total 4\n", "etl_rpc_budget_remaining 0\n", "etl_rpc_budget_limit 8\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatal("missing metric", want)
		}
	}
	b.Reset()
	r.ConfigureRPC(0)
	r.writeRPC(&b)
	if !strings.Contains(b.String(), "etl_rpc_budget_enabled 0\n") || strings.Contains(b.String(), "etl_rpc_budget_remaining") {
		t.Fatal("unbounded budget misreported")
	}
}
