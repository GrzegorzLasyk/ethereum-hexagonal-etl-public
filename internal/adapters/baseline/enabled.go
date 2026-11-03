//go:build perfbaseline

package baseline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Runtime struct {
	UserCPUSeconds       float64 `json:"user_cpu_seconds"`
	SystemCPUSeconds     float64 `json:"system_cpu_seconds"`
	MaxRSSBytes          int64   `json:"process_high_water_rss_bytes"`
	PeakHeapBytes        uint64  `json:"sampled_peak_heap_bytes"`
	AllocBytes           uint64  `json:"allocated_bytes"`
	Mallocs              uint64  `json:"allocations"`
	GCCount              uint32  `json:"gc_cycles"`
	GCPauseSeconds       float64 `json:"gc_pause_seconds"`
	PoolMax              int32   `json:"pool_max_connections"`
	PoolPeakAcquired     int32   `json:"pool_sampled_peak_acquired"`
	PoolPeakTotal        int32   `json:"pool_sampled_peak_total"`
	PoolAcquires         int64   `json:"pool_acquires"`
	PoolAcquireSeconds   float64 `json:"pool_acquire_seconds"`
	PoolEmptyAcquires    int64   `json:"pool_empty_acquires"`
	PoolEmptyWaitSeconds float64 `json:"pool_empty_wait_seconds"`
	PoolCanceledAcquires int64   `json:"pool_canceled_acquires"`
}
type Result struct {
	DirectQueries   uint64                  `json:"direct_query_calls"`
	BatchCalls      uint64                  `json:"batch_calls"`
	BatchStatements uint64                  `json:"queued_batch_statements"`
	BatchErrors     uint64                  `json:"batch_errors"`
	Batches         Distribution            `json:"client_batch_latency"`
	Scope           string                  `json:"scope"`
	Runtime         Runtime                 `json:"runtime"`
	Queries         map[string]Distribution `json:"client_query_latency"`
	Transactions    Distribution            `json:"client_transaction_latency"`
	SQLWrites       uint64                  `json:"successful_insert_update_delete_statements"`
	QueryErrors     uint64                  `json:"query_errors"`
}
type Probe struct {
	mu         sync.Mutex
	file       *os.File
	pool       *pgxpool.Pool
	initial    runtime.MemStats
	initialCPU syscall.Rusage
	result     Result
	tx         map[*pgx.Conn]time.Time
	done       chan struct{}
	joined     chan struct{}
	once       sync.Once
}
type traceKey struct{}
type query struct {
	start time.Time
	kind  string
}

func Configure(config *pgxpool.Config) (*Probe, error) {
	path := os.Getenv("ETL_BASELINE_PROBE")
	if path == "" {
		return &Probe{}, nil
	}
	if os.Getenv("ETL_BASELINE_OPT_IN") != "1" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" || config.ConnConfig.Tracer != nil {
		return nil, errors.New("baseline instrumentation requires explicit local opt-in")
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, errors.New("cannot create exclusive baseline probe")
	}
	p := &Probe{file: f, tx: map[*pgx.Conn]time.Time{}, done: make(chan struct{}), joined: make(chan struct{})}
	p.result = Result{Scope: "whole_process_after_pool_configuration_including_startup_and_observation", Queries: map[string]Distribution{}}
	runtime.ReadMemStats(&p.initial)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &p.initialCPU)
	config.ConnConfig.Tracer = p
	return p, nil
}
func (p *Probe) Attach(pool *pgxpool.Pool) {
	if p.file == nil {
		return
	}
	p.pool = pool
	go func() {
		defer close(p.joined)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-tick.C:
				p.sample()
			}
		}
	}()
}
func (p *Probe) sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	p.mu.Lock()
	defer p.mu.Unlock()
	r := &p.result.Runtime
	r.PeakHeapBytes = max(r.PeakHeapBytes, m.HeapAlloc)
	r.AllocBytes = m.TotalAlloc - p.initial.TotalAlloc
	r.Mallocs = m.Mallocs - p.initial.Mallocs
	r.GCCount = m.NumGC - p.initial.NumGC
	r.GCPauseSeconds = float64(m.PauseTotalNs-p.initial.PauseTotalNs) / 1e9
	if p.pool != nil {
		s := p.pool.Stat()
		r.PoolMax = s.MaxConns()
		r.PoolPeakAcquired = max(r.PoolPeakAcquired, s.AcquiredConns())
		r.PoolPeakTotal = max(r.PoolPeakTotal, s.TotalConns())
		r.PoolAcquires = s.AcquireCount()
		r.PoolAcquireSeconds = s.AcquireDuration().Seconds()
		r.PoolEmptyAcquires = s.EmptyAcquireCount()
		r.PoolEmptyWaitSeconds = s.EmptyAcquireWaitTime().Seconds()
		r.PoolCanceledAcquires = s.CanceledAcquireCount()
	}
}
func (p *Probe) Close() {
	if p.file == nil {
		return
	}
	p.once.Do(func() {
		if p.pool != nil {
			close(p.done)
			<-p.joined
		}
		p.sample()
		var usage syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
		seconds := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
		p.result.Runtime.UserCPUSeconds = seconds(usage.Utime) - seconds(p.initialCPU.Utime)
		p.result.Runtime.SystemCPUSeconds = seconds(usage.Stime) - seconds(p.initialCPU.Stime)
		rss := usage.Maxrss
		if runtime.GOOS == "linux" {
			rss *= 1024
		}
		p.result.Runtime.MaxRSSBytes = rss
		for k, v := range p.result.Queries {
			p.result.Queries[k] = v.Summary()
		}
		p.result.Transactions = p.result.Transactions.Summary()
		p.result.Batches = p.result.Batches.Summary()
		_ = json.NewEncoder(p.file).Encode(p.result)
		_ = p.file.Close()
	})
}
func (p *Probe) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	kind := "other"
	fields := strings.Fields(d.SQL)
	if len(fields) > 0 {
		switch strings.ToLower(fields[0]) {
		case "select", "insert", "update", "delete", "begin", "commit", "rollback":
			kind = strings.ToLower(fields[0])
		}
	}
	return context.WithValue(ctx, traceKey{}, query{time.Now(), kind})
}
func (p *Probe) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	q, ok := ctx.Value(traceKey{}).(query)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.result.DirectQueries++
	stat := p.result.Queries[q.kind]
	stat.Add(time.Since(q.start).Seconds())
	p.result.Queries[q.kind] = stat
	if d.Err != nil {
		p.result.QueryErrors++
	} else if d.CommandTag.Insert() || d.CommandTag.Update() || d.CommandTag.Delete() {
		p.result.SQLWrites++
	}
	if q.kind == "begin" && d.Err == nil {
		p.tx[c] = q.start
	}
	if q.kind == "commit" || q.kind == "rollback" {
		if start, ok := p.tx[c]; ok {
			p.result.Transactions.Add(time.Since(start).Seconds())
			delete(p.tx, c)
		}
	}
}

// Batch query callbacks do not represent independent round trips. Only measure
// the entire SendBatch/Close interval; do not fabricate per-statement latencies.
type batchTraceKey struct{}

var _ pgx.BatchTracer = (*Probe)(nil)

func (p *Probe) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	p.mu.Lock()
	p.result.BatchCalls++
	p.result.BatchStatements += uint64(d.Batch.Len())
	p.mu.Unlock()
	return context.WithValue(ctx, batchTraceKey{}, time.Now())
}
func (p *Probe) TraceBatchQuery(_ context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d.Err != nil {
		p.result.QueryErrors++
	} else if d.CommandTag.Insert() || d.CommandTag.Update() || d.CommandTag.Delete() {
		p.result.SQLWrites++
	}
}
func (p *Probe) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchEndData) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if start, ok := ctx.Value(batchTraceKey{}).(time.Time); ok {
		p.result.Batches.Add(time.Since(start).Seconds())
	}
	if d.Err != nil {
		p.result.BatchErrors++
	}
}
