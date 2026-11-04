//go:build perfbaseline

// enabled_test.go exercises opt-in runtime, pgx and pool instrumentation for offline baselines in
// the shared technical support layer, outside service domains.

package baseline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestProbeOptInAndExclusiveOutput requires explicit baseline instrumentation consent and refuses
// overwriting an existing evidence file.
func TestProbeOptInAndExclusiveOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.json")
	t.Setenv("ETL_BASELINE_PROBE", path)
	t.Setenv("ETL_BASELINE_OPT_IN", "")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	config, e := pgxpool.ParseConfig("")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := Configure(config); e == nil {
		t.Fatal("missing opt-in accepted")
	}
	t.Setenv("ETL_BASELINE_OPT_IN", "1")
	t.Setenv("CI", "true")
	if _, e := Configure(config); e == nil {
		t.Fatal("CI accepted")
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("guard created output")
	}
	t.Setenv("CI", "")
	p, e := Configure(config)
	if e != nil {
		t.Fatal(e)
	}
	secret := "private-secret-do-not-record"
	ctx := p.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "WITH secret AS (SELECT '" + secret + "') UPDATE own_table SET v=$1", Args: []any{secret}})
	p.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("UPDATE 3")})
	p.Close()
	p.Close()
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(data), secret) || strings.Contains(string(data), "own_table") {
		t.Fatal("SQL/arguments leaked")
	}
	var report Result
	if json.Unmarshal(data, &report) != nil || report.SQLWrites != 1 || report.Queries["other"].Count != 1 {
		t.Fatal("missing CTE write instrumentation")
	}
	config.ConnConfig.Tracer = nil
	if _, e := Configure(config); e == nil {
		t.Fatal("overwrote existing evidence")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("unsafe report mode")
	}
}

// TestBatchProbeCountsWithoutRecordingSQL checks pgx submission/statement counters while ensuring
// SQL text and argument values are absent from evidence.
func TestBatchProbeCountsWithoutRecordingSQL(t *testing.T) {
	p := &Probe{result: Result{Queries: map[string]Distribution{}}}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			b := &pgx.Batch{}
			b.Queue("INSERT secret payload", "private")
			b.Queue("INSERT secret payload", "private")
			ctx := p.TraceBatchStart(t.Context(), nil, pgx.TraceBatchStartData{Batch: b})
			p.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{CommandTag: pgconn.NewCommandTag("INSERT 0 1")})
			p.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: errors.New("secret server detail")})
			p.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: errors.New("secret server detail")})
		})
	}
	wg.Wait()
	if p.result.BatchCalls != 4 || p.result.BatchStatements != 8 || p.result.SQLWrites != 4 || p.result.QueryErrors != 4 || p.result.BatchErrors != 4 || p.result.Batches.Count != 4 || p.result.DirectQueries != 0 || len(p.result.Queries) != 0 {
		t.Fatal("incorrect batch measurements", p.result)
	}
	data, err := json.Marshal(p.result)
	if err != nil || strings.Contains(string(data), "secret") || strings.Contains(string(data), "private") {
		t.Fatal("probe disclosure", err)
	}
}
