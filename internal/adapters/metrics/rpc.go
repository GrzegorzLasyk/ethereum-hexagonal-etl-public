package metrics

import (
	"bytes"
	"fmt"
)

type rpcMetrics struct {
	enabled                  bool
	limit, attempts, retries uint64
}

// One registry belongs to one service process and its single acquisition client.
func (r *Registry) ConfigureRPC(limit uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rpc.enabled = true
	r.rpc.limit = limit
}
func (r *Registry) RPCAttempt(retry bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rpc.attempts++
	if retry {
		r.rpc.retries++
	}
}
func (r *Registry) writeRPC(b *bytes.Buffer) {
	if !r.rpc.enabled {
		return
	}
	a := r.rpc
	family(b, "etl_rpc_attempts_total", "counter", "HTTP dispatch attempts reserved since process start, including failures; not provider billing.")
	fmt.Fprintf(b, "etl_rpc_attempts_total %d\n", a.attempts)
	family(b, "etl_rpc_retries_total", "counter", "HTTP retry attempts within a logical RPC call since process start; excludes new worker cycles.")
	fmt.Fprintf(b, "etl_rpc_retries_total %d\n", a.retries)
	gauge(b, "etl_rpc_budget_enabled", "1 for a finite process-local RPC attempt budget.", flag(a.limit > 0))
	if a.limit > 0 {
		gauge(b, "etl_rpc_budget_limit", "Configured maximum HTTP attempts for this process.", a.limit)
		remaining := uint64(0)
		if a.attempts < a.limit {
			remaining = a.limit - a.attempts
		}
		gauge(b, "etl_rpc_budget_remaining", "Remaining reserved HTTP attempts; resets on restart.", remaining)
	}
}
