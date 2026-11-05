//go:build postgresintegration && perfbaseline

// performance_test.go exercises private operator performance-setting validation without automatic
// tuning in the offline test and diagnostic infrastructure.

package handoff_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/baseline"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/test/performance"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// baselineEnvironment builds isolated host-service settings for the selected worker/resource
// scenario without enabling live RPC.
func baselineEnvironment(service string) []string {
	var env []string
	for _, entry := range os.Environ() {
		k, _, _ := strings.Cut(entry, "=")
		blocked := false
		for _, prefix := range []string{"EXTRACTOR_", "TRANSFORMER_", "LOADER_", "HANDOFF_", "PG", "POSTGRES_", "ETHEREUM_", "REAL_FIXTURE_", "ETL_BASELINE_"} {
			blocked = blocked || strings.HasPrefix(k, prefix)
		}
		if !blocked && k != "LOG_LEVEL" {
			env = append(env, entry)
		}
	}
	return append(env, strings.ToUpper(service)+"_DATABASE_URL="+os.Getenv(strings.ToUpper(service)+"_TEST_DATABASE_URL"), strings.ToUpper(service)+"_CHAIN_ID=1", "LOG_LEVEL=INFO")
}

// baselinePort reserves a test-local port for one measured service/proxy endpoint.
func baselinePort(t *testing.T) string {
	l := reserve(t)
	address := l.Addr().String()
	l.Close()
	return address
}

// scrape serves the metrics handler through a controlled HTTP recorder and returns its exposition
// for assertions.
func scrape(address, path string) (string, error) {
	client := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get("http://" + address + path)
	if err != nil {
		return "", fmt.Errorf("local metrics unavailable")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != 200 {
		return "", fmt.Errorf("local endpoint not ready")
	}
	return string(data), nil
}

// baselineSeedCommand builds the offline source invocation for initial acquisition or duplicate
// replay without external transport.
func baselineSeedCommand(t *testing.T, s performance.Scenario, duplicate bool) *exec.Cmd {
	t.Helper()
	name := "source"
	if duplicate {
		name = "duplicate-source"
	}
	cmd := exec.CommandContext(t.Context(), filepath.Join(s.Binaries, "source.test"), "-test.run=^TestSeedPerformance$", "-test.count=1", "-test.timeout=4m", "-test.v")
	cmd.Env = append(baselineEnvironment("extractor"), "EXTRACTOR_TEST_DATABASE_URL="+os.Getenv("EXTRACTOR_TEST_DATABASE_URL"), "HANDOFF_TEST_ISOLATED=1", "ETL_BASELINE_OPT_IN=1", "ETL_BASELINE_SCENARIO="+os.Getenv("ETL_BASELINE_SCENARIO"), "ETL_BASELINE_PROBE="+filepath.Join(s.Directory, name+"-probe.json"))
	if duplicate {
		cmd.Env = append(cmd.Env, "ETL_BASELINE_DUPLICATE=1")
	}
	return cmd
}

// TestPerformanceBaseline measures only a fully verified isolated offline pipeline and rejects
// incomplete data before reporting throughput.
func TestPerformanceBaseline(t *testing.T) {
	s, err := performance.ReadScenario()
	if err != nil || os.Getenv("HANDOFF_TEST_ISOLATED") != "1" {
		t.Fatal("requires opt-in baseline runner")
	}
	r := performance.Report{Version: "stage14.1/v1", Status: "failed", Scenario: s, Environment: map[string]any{"go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "poll_interval_ms": 20, "observation_interval_ms": 100, "metrics_interval_ms": 250, "extractor_acquisition_concurrency": 1}, RPC: map[string]performance.RPC{}, Processes: map[string]json.RawMessage{}, Operations: map[string]baseline.Distribution{}, PostgreSQL: map[string]any{}, Integrity: map[string]bool{}}
	r.Limitations = []string{"Offline in-memory JSON-RPC decoding; no provider network/pacing/head discovery/reorg scheduling cost.", "Source production ExtractBlock use case commits sequentially; authentic heights have separate databases.", "gRPC uses an observing proxy hop; forwarded ACK latency differs from direct publisher latency.", "End-to-end latency uses same-host source start and first observed Loader completion, with 100ms polling uncertainty plus query time.", "Process probes include startup, observers and post-measurement duplicate checks; GC/SQL tracing adds overhead.", "PostgreSQL timings are client-observed, not EXPLAIN/server execution; statistics can lag and WAL is cluster-wide.", "Warmups use separate fresh projects; they warm host/tool caches, not the next sample's PostgreSQL data cache.", "No inference about higher worker counts from single-block authentic runs; synthetic larger ranges supply concurrency.", "Reorg history is not mutated; reorg recovery performance and fault-injection throughput are outside this baseline."}
	defer func() {
		if t.Failed() {
			r.Status = "failed"
			r.BlocksPerSecond = nil
			r.TransactionsPerSecond = nil
		}
		if performance.ExclusiveJSON(filepath.Join(s.Directory, "report.json"), r) != nil {
			t.Error("cannot persist baseline report")
		}
	}()
	pools := map[string]*pgxpool.Pool{"extractor": pool(t, "extractor"), "transformer": pool(t, "transformer"), "loader": pool(t, "loader")}
	for service, p := range pools {
		table := "inbox_events"
		if service == "extractor" {
			table = "raw_blocks"
		}
		if queryInt(t, p, "SELECT count(*) FROM "+service+"."+table) != 0 {
			t.Fatal("baseline requires empty isolated storage")
		}
	}
	trAddress, loAddress := baselinePort(t), baselinePort(t)
	rawListener, transformedListener := reserve(t), reserve(t)
	raw := &measuredRaw{client: etlv1.NewTransformerServiceClient(connection(t, trAddress))}
	normalized := &measuredTransformed{client: etlv1.NewLoaderServiceClient(connection(t, loAddress))}
	serve(t, rawListener, func(server *grpc.Server) { etlv1.RegisterTransformerServiceServer(server, raw) })
	serve(t, transformedListener, func(server *grpc.Server) { etlv1.RegisterLoaderServiceServer(server, normalized) })
	children := map[string]*child{}
	health := map[string]string{}
	for _, service := range []string{"loader", "transformer", "extractor"} {
		prefix := strings.ToUpper(service)
		health[service] = baselinePort(t)
		env := append(baselineEnvironment(service), "ETL_BASELINE_OPT_IN=1", "ETL_BASELINE_PROBE="+filepath.Join(s.Directory, service+"-probe.json"), prefix+"_HEALTH_ADDRESS="+health[service])
		if service == "loader" {
			env = append(env, "LOADER_SQL_BATCH_SIZE="+strconv.Itoa(s.LoaderBatchSize))
		}
		if service == "extractor" {
			env = append(env, "EXTRACTOR_MODE=dispatch", "EXTRACTOR_RPC_URL=")
		}
		if service != "extractor" {
			address := loAddress
			if service == "transformer" {
				address = trAddress
			}
			env = append(env, prefix+"_GRPC_ADDRESS="+address, prefix+"_WORKERS="+strconv.Itoa(s.Workers), prefix+"_POLL_INTERVAL=20ms")
		}
		if service != "loader" {
			address := rawListener.Addr().String()
			if service == "transformer" {
				address = transformedListener.Addr().String()
			}
			env = append(env, prefix+"_DISPATCH_TARGET="+address, prefix+"_DISPATCH_WORKERS="+strconv.Itoa(s.Workers), prefix+"_DISPATCH_POLL_INTERVAL=20ms")
		}
		children[service] = launch(t, filepath.Join(s.Binaries, service), env)
		await(t, service+" readiness", func() bool { _, e := scrape(health[service], "/ready"); return e == nil })
	}
	pgBefore := postgresSample(t, pools)
	r.PostgreSQL["before"] = pgBefore
	completed := map[string]time.Time{}
	sample := func() {
		point := performance.Sample{At: time.Now(), Completed: len(completed), Metrics: map[string]string{}}
		for service, address := range health {
			body, e := scrape(address, "/metrics")
			if e != nil || !strings.Contains(body, "etl_database_snapshot_success 1\n") {
				t.Fatal("baseline metric snapshot unavailable")
			}
			point.Metrics[service] = body
		}
		var height *string
		if pools["extractor"].QueryRow(t.Context(), "SELECT max(block_number)::text FROM extractor.extraction_checkpoints").Scan(&height) != nil {
			t.Fatal("checkpoint observation unavailable")
		}
		if height != nil {
			n, e := strconv.ParseUint(*height, 10, 64)
			if e != nil {
				t.Fatal("checkpoint observation invalid")
			}
			point.Checkpoint = &n
		}
		r.Samples = append(r.Samples, point)
	}
	sample()
	cmd := baselineSeedCommand(t, s, false)
	var sourceOutput bytes.Buffer
	cmd.Stdout = &sourceOutput
	cmd.Stderr = &sourceOutput
	if cmd.Start() != nil {
		t.Fatal("cannot start offline source")
	}
	sourceDone := make(chan error, 1)
	go func() { sourceDone <- cmd.Wait() }()
	sourceFinished := false
	defer func() {
		if !sourceFinished {
			_ = cmd.Process.Kill()
			<-sourceDone
		}
	}()
	deadline := time.NewTimer(5 * time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	lastSample := time.Now()
	for {
		if !sourceFinished {
			select {
			case e := <-sourceDone:
				sourceFinished = true
				if e != nil {
					t.Fatal("offline source failed (output suppressed)")
				}
			default:
			}
		}
		rows, e := pools["loader"].Query(t.Context(), "SELECT event_id FROM loader.inbox_events WHERE status='completed'")
		if e != nil {
			t.Fatal("completion query unavailable")
		}
		for rows.Next() {
			var id string
			if rows.Scan(&id) != nil {
				t.Fatal("completion identity unavailable")
			}
			if _, ok := completed[id]; !ok {
				completed[id] = time.Now()
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal("incomplete completion observation")
		}
		done := true
		for _, queue := range []struct{ service, table, terminal string }{{"extractor", "outbox_events", "delivered"}, {"transformer", "inbox_events", "completed"}, {"transformer", "outbox_events", "delivered"}, {"loader", "inbox_events", "completed"}} {
			p := pools[queue.service]
			table := queue.service + "." + queue.table
			if queryInt(t, p, "SELECT count(*) FROM "+table+" WHERE status='failed'") > 0 {
				t.Fatal("terminal event: benchmark is not successful")
			}
			done = done && queryInt(t, p, "SELECT count(*) FROM "+table+" WHERE status='"+queue.terminal+"'") == s.Blocks && queryInt(t, p, "SELECT count(*) FROM "+table) == s.Blocks
		}
		// Queue counts can observe a commit made after the completed-ID query.
		// Wait for its next observation before calculating every event's latency.
		if sourceFinished && done && len(completed) == s.Blocks {
			break
		}
		if time.Since(lastSample) >= 250*time.Millisecond {
			sample()
			lastSample = time.Now()
		}
		select {
		case <-deadline.C:
			t.Fatal("baseline timed out with incomplete durable work")
		case <-t.Context().Done():
			t.Fatal("baseline canceled")
		case <-tick.C:
		}
	}
	ended := time.Now()
	sample()
	r.PostgreSQL["after"] = postgresSample(t, pools)
	var wal string
	if pools["extractor"].QueryRow(t.Context(), "SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::text", r.PostgreSQL["after"].(map[string]any)["wal_lsn"], pgBefore["wal_lsn"]).Scan(&wal) != nil {
		t.Fatal("WAL delta unavailable")
	}
	r.PostgreSQL["wal_bytes_delta"] = wal
	data, e := os.ReadFile(filepath.Join(s.Directory, "source.json"))
	if e != nil || json.Unmarshal(data, &r.Seed) != nil || len(r.Seed.Events) != s.Blocks {
		t.Fatal("incomplete source measurement")
	}
	for _, event := range r.Seed.Events {
		r.ExtractionLatency.Add(event.DurationSeconds)
		id := strings.Replace(event.EventID, "raw-block/", "transformed-block/", 1)
		at, ok := completed[id]
		if !ok || at.Before(event.Started) {
			t.Fatal("invalid completion latency")
		}
		r.E2ELatency.Add(at.Sub(event.Started).Seconds())
	}
	r.ExtractionLatency = r.ExtractionLatency.Summary()
	r.E2ELatency = r.E2ELatency.Summary()
	r.RPC["extractor_to_transformer"] = raw.meter.snapshot()
	r.RPC["transformer_to_loader"] = normalized.meter.snapshot()
	// Duplicate checks are outside the throughput/latency window.
	raw.mu.Lock()
	lastRaw := raw.last
	raw.mu.Unlock()
	normalized.mu.Lock()
	lastNormalized := normalized.last
	normalized.mu.Unlock()
	ack, e := raw.client.SubmitRawBlock(t.Context(), lastRaw)
	if e != nil || ack.GetAcceptance().GetStatus() != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
		t.Fatal("raw deduplication failed")
	}
	normalizedACK, e := normalized.client.SubmitTransformedBlock(t.Context(), lastNormalized)
	if e != nil || normalizedACK.GetAcceptance().GetStatus() != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
		t.Fatal("normalized deduplication failed")
	}
	duplicate := baselineSeedCommand(t, s, true)
	if _, e := duplicate.CombinedOutput(); e != nil {
		t.Fatal("source duplicate failed")
	}
	assertBaselineData(t, s, pools, &r.Seed)
	r.Integrity = map[string]bool{"raw_bytes": true, "ordered_checkpoint": true, "durable_handoffs": true, "normalized_fields": true, "canonical_unknown": true, "deduplication": true, "history_preserved": true}
	for _, service := range []string{"extractor", "transformer", "loader"} {
		children[service].stop(t, false)
		addOperations(t, &r, service, children[service].output.Bytes())
	}
	for _, key := range []string{"extractor/dispatch_outbox", "transformer/accept_inbox", "transformer/process_inbox", "transformer/dispatch_outbox", "loader/accept_inbox", "loader/process_inbox"} {
		if r.Operations[key].Count == 0 {
			t.Fatal("missing operation latency evidence", key)
		}
	}
	for _, service := range []string{"extractor", "transformer", "loader", "source"} {
		data, e := os.ReadFile(filepath.Join(s.Directory, service+"-probe.json"))
		if e != nil || !json.Valid(data) {
			t.Fatal("missing process telemetry")
		}
		var probe baseline.Result
		if json.Unmarshal(data, &probe) != nil || probe.Runtime.PoolMax == 0 || probe.Runtime.PoolAcquires == 0 || len(probe.Queries) == 0 {
			t.Fatal("incomplete process telemetry", service)
		}
		r.Processes[service] = data
	}
	if e := r.Accept(ended.Sub(r.Seed.Events[0].Started), s.Blocks, r.Seed.Transactions); e != nil {
		t.Fatal(e)
	}
	t.Logf("baseline verified dataset=%s blocks=%d workers=%d seconds=%.6f blocks/s=%.3f transactions/s=%.3f", s.Dataset, s.Blocks, s.Workers, r.MeasuredSeconds, *r.BlocksPerSecond, *r.TransactionsPerSecond)
}

// addOperations imports process probe output into the baseline report and rejects missing/invalid
// evidence.
func addOperations(t *testing.T, r *performance.Report, service string, output []byte) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var row struct {
			Operation string  `json:"operation"`
			Duration  float64 `json:"duration_ms"`
		}
		if json.Unmarshal(scanner.Bytes(), &row) != nil {
			continue
		}
		switch row.Operation {
		case "process_inbox", "dispatch_outbox", "accept_inbox":
			key := service + "/" + row.Operation
			v := r.Operations[key]
			v.Add(row.Duration / 1000)
			r.Operations[key] = v
		}
	}
	if scanner.Err() != nil {
		t.Fatal("operation log measurement incomplete")
	}
	for k, v := range r.Operations {
		if strings.HasPrefix(k, service+"/") {
			r.Operations[k] = v.Summary()
		}
	}
}

// postgresSample reads diagnostic PostgreSQL counters/WAL and owning pool state for a bounded
// measurement snapshot.
func postgresSample(t *testing.T, pools map[string]*pgxpool.Pool) map[string]any {
	t.Helper()
	out := map[string]any{}
	var version, wal, shared, work string
	if pools["extractor"].QueryRow(t.Context(), "SELECT version(),pg_current_wal_insert_lsn()::text,current_setting('shared_buffers'),current_setting('work_mem')").Scan(&version, &wal, &shared, &work) != nil {
		t.Fatal("PostgreSQL observation unavailable")
	}
	out["version"] = version
	out["wal_lsn"] = wal
	out["shared_buffers"] = shared
	out["work_mem"] = work
	for service, p := range pools {
		var inserts, updates, deletes, bytes int64
		if p.QueryRow(t.Context(), "SELECT COALESCE(sum(n_tup_ins),0)::bigint,COALESCE(sum(n_tup_upd),0)::bigint,COALESCE(sum(n_tup_del),0)::bigint FROM pg_stat_user_tables WHERE schemaname=$1", service).Scan(&inserts, &updates, &deletes) != nil {
			t.Fatal("own-schema write statistics unavailable")
		}
		if p.QueryRow(t.Context(), "SELECT pg_database_size(current_database())").Scan(&bytes) != nil {
			t.Fatal("database size unavailable")
		}
		out[service] = map[string]int64{"rows_inserted_eventually_reported": inserts, "rows_updated_eventually_reported": updates, "rows_deleted_eventually_reported": deletes, "database_bytes": bytes}
	}
	return out
}
