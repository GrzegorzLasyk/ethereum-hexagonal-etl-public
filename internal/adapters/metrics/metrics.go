// Package metrics implements bounded, process-local Prometheus text exposition.
// It is adapter infrastructure, never a domain/application dependency.
package metrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

var bounds = [...]float64{.001, .01, .1, .5, 1, 5, 30, 120}
var outcomes = [...]string{"completed", "delivered", "retry", "failed", "interrupted", "unknown", "waiting", "backpressure", "done", "rewound", "progress"}
var operations = map[string]bool{"process_inbox": true, "dispatch_outbox": true, "claim_inbox": true, "claim_outbox": true, "persist_block": true, "index_cycle": true, "reindex_cycle": true, "index_block": true, "reindex_block": true}

type operation struct {
	Counts            map[string]uint64
	Buckets           [len(bounds)]uint64
	Count             uint64
	Sum               float64
	Retries, Failures uint64
}
type Registry struct {
	mu              sync.Mutex
	ops             map[string]*operation
	started         time.Time
	head            uint64
	headAt, cycleAt time.Time
	backpressure    bool
	pauses          uint64
	lastSnapshot    time.Time
	collecting      chan struct{}
}

func New(names ...string) *Registry {
	r := &Registry{ops: map[string]*operation{}, started: time.Now(), collecting: make(chan struct{}, 1)}
	for _, name := range names {
		if operations[name] {
			r.ops[name] = &operation{Counts: map[string]uint64{}}
		}
	}
	return r
}

// Observe accepts only fixed registered operations/outcomes. No identity or error
// string can create a series. Nil receivers support uninstrumented test callers.
func (r *Registry) Observe(name, outcome string, d time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	op, ok := r.ops[name]
	if !ok {
		return
	}
	valid := false
	for _, s := range outcomes {
		if outcome == s {
			valid = true
			break
		}
	}
	if !valid {
		return
	}
	seconds := d.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	op.Counts[outcome]++
	op.Count++
	op.Sum += seconds
	for i, b := range bounds {
		if seconds <= b {
			op.Buckets[i]++
		}
	}
	if outcome == "retry" {
		op.Retries++
	}
	if outcome == "retry" || outcome == "failed" || outcome == "interrupted" || outcome == "unknown" {
		op.Failures++
	}
}
func (r *Registry) Head(n uint64, at time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if at.After(r.headAt) {
		r.head = n
		r.headAt = at
	}
}
func (r *Registry) Cycle(backpressure bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backpressure = backpressure
	r.cycleAt = time.Now()
	if backpressure {
		r.pauses++
	}
}

// Snapshot is a technical exposition DTO, mapped from service-owned ports.
// Backlog includes failed retained work; Done is a current durable row count.
type Queue struct {
	Kind                            string
	Pending, InFlight, Failed, Done int64
	OldestAge                       float64
}
type Snapshot struct {
	Queues     []Queue
	Extractor  bool
	Checkpoint *uint64
	Revision   int64
}

func family(b *bytes.Buffer, name, kind, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}
func gauge(b *bytes.Buffer, name, help string, v any) {
	family(b, name, "gauge", help)
	fmt.Fprintf(b, "%s %v\n", name, v)
}
func flag(v bool) int {
	if v {
		return 1
	}
	return 0
}

// Handler samples the owning database on demand, with a one-second deadline and
// one concurrent query. Failure omits all database gauges, never substitutes 0.
// HTTP 200 keeps process metrics visible; database_snapshot_success MUST be used
// to evaluate database-derived telemetry. No raw error is exposed.
func (r *Registry) Handler(read func(context.Context) (Snapshot, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var snapshot Snapshot
		success := false
		select {
		case r.collecting <- struct{}{}:
			ctx, cancel := context.WithTimeout(req.Context(), time.Second)
			s, err := read(ctx)
			cancel()
			<-r.collecting
			if err == nil && ctx.Err() != context.DeadlineExceeded && req.Context().Err() == nil {
				snapshot = s
				success = true
			}
		default:
		}
		r.mu.Lock()
		if success {
			r.lastSnapshot = time.Now()
		}
		var b bytes.Buffer
		gauge(&b, "etl_process_start_time_seconds", "Process start Unix time in seconds.", float64(r.started.UnixNano())/1e9)
		gauge(&b, "etl_database_snapshot_success", "1 only when this scrape read a complete database snapshot.", flag(success))
		last := float64(0)
		if !r.lastSnapshot.IsZero() {
			last = float64(r.lastSnapshot.UnixNano()) / 1e9
		}
		gauge(&b, "etl_database_snapshot_last_success_timestamp_seconds", "Last successful database snapshot Unix time; 0 before first success.", last)
		r.writeOperations(&b)
		if !r.headAt.IsZero() {
			gauge(&b, "etl_extractor_head", "Last successfully observed provider head height.", r.head)
			gauge(&b, "etl_extractor_head_observed_timestamp_seconds", "Provider head observation Unix time.", float64(r.headAt.UnixNano())/1e9)
		}
		if !r.cycleAt.IsZero() {
			gauge(&b, "etl_extractor_backpressure", "Whether the last indexing cycle was paused by capacity.", flag(r.backpressure))
			gauge(&b, "etl_extractor_cycle_timestamp_seconds", "Last successful indexing cycle Unix time.", float64(r.cycleAt.UnixNano())/1e9)
			family(&b, "etl_extractor_backpressure_total", "counter", "Indexing cycles paused by capacity since process start.")
			fmt.Fprintf(&b, "etl_extractor_backpressure_total %d\n", r.pauses)
		}
		if success {
			r.writeSnapshot(&b, snapshot)
		}
		r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(b.Bytes())
	})
}
func (r *Registry) writeOperations(b *bytes.Buffer) {
	names := make([]string, 0, len(r.ops))
	for n := range r.ops {
		names = append(names, n)
	}
	sort.Strings(names)
	family(b, "etl_operations_total", "counter", "Returned operations by local outcome since process start; idle claims excluded.")
	for _, n := range names {
		for _, o := range outcomes {
			fmt.Fprintf(b, "etl_operations_total{operation=%q,outcome=%q} %d\n", n, o, r.ops[n].Counts[o])
		}
	}
	family(b, "etl_operation_retries_total", "counter", "Returned operations that durably scheduled retry since process start.")
	for _, n := range names {
		fmt.Fprintf(b, "etl_operation_retries_total{operation=%q} %d\n", n, r.ops[n].Retries)
	}
	family(b, "etl_operation_failures_total", "counter", "Unsuccessful returned operations including retry and unknown outcomes.")
	for _, n := range names {
		fmt.Fprintf(b, "etl_operation_failures_total{operation=%q} %d\n", n, r.ops[n].Failures)
	}
	family(b, "etl_operation_duration_seconds", "histogram", "Local operation duration in seconds, including unsuccessful operations.")
	for _, n := range names {
		op := r.ops[n]
		for i, v := range bounds {
			fmt.Fprintf(b, "etl_operation_duration_seconds_bucket{operation=%q,le=\"%g\"} %d\n", n, v, op.Buckets[i])
		}
		fmt.Fprintf(b, "etl_operation_duration_seconds_bucket{operation=%q,le=\"+Inf\"} %d\netl_operation_duration_seconds_sum{operation=%q} %g\netl_operation_duration_seconds_count{operation=%q} %d\n", n, op.Count, n, op.Sum, n, op.Count)
	}
}
func (r *Registry) writeSnapshot(b *bytes.Buffer, s Snapshot) {
	family(b, "etl_queue_events", "gauge", "Durable row count by queue and state at scrape time.")
	for _, q := range s.Queues {
		if q.Kind != "inbox" && q.Kind != "outbox" {
			continue
		}
		for i, state := range []string{"pending", "in_flight", "failed", "done"} {
			fmt.Fprintf(b, "etl_queue_events{queue=%q,state=%q} %d\n", q.Kind, state, []int64{q.Pending, q.InFlight, q.Failed, q.Done}[i])
		}
	}
	family(b, "etl_queue_backlog", "gauge", "All retained unfinished events including in-flight and failed work.")
	for _, q := range s.Queues {
		if q.Kind == "inbox" || q.Kind == "outbox" {
			fmt.Fprintf(b, "etl_queue_backlog{queue=%q} %d\n", q.Kind, q.Pending+q.InFlight+q.Failed)
		}
	}
	family(b, "etl_queue_oldest_pending_age_seconds", "gauge", "Age since first local acceptance/creation of oldest unfinished event, including failed work.")
	for _, q := range s.Queues {
		if q.Kind == "inbox" || q.Kind == "outbox" {
			fmt.Fprintf(b, "etl_queue_oldest_pending_age_seconds{queue=%q} %g\n", q.Kind, q.OldestAge)
		}
	}
	if s.Extractor {
		gauge(b, "etl_extractor_checkpoint_initialized", "Whether a durable checkpoint exists.", flag(s.Checkpoint != nil))
		if s.Checkpoint != nil {
			gauge(b, "etl_extractor_checkpoint", "Durable checkpoint height.", *s.Checkpoint)
			gauge(b, "etl_extractor_revision", "Durable rewind revision.", s.Revision)
			if !r.headAt.IsZero() {
				gauge(b, "etl_extractor_lag_blocks", "Last observed head minus current checkpoint; negative means head below checkpoint.", float64(r.head)-float64(*s.Checkpoint))
			}
		}
	}
}
