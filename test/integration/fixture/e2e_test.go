//go:build composeintegration

// e2e_test.go exercises full local synthetic pipeline outages, retries, reorg and recovery in the
// offline test and diagnostic infrastructure.

// Package fixture_test provides full local synthetic pipeline outages, retries, reorg and recovery
// in the offline test and diagnostic infrastructure.
package fixture_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/test/fixture"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The harness is an external observer. Every SQL connection authenticates as
// exactly one service runtime; no application receives another service's DSN.
type system struct {
	t          *testing.T
	root, url  string
	base       []string
	ex, tr, lo *pgxpool.Pool
}

// command runs a bounded command in the disposable fixture project, keeping environment details
// out of failure output.
func (s *system) command(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(s.t.Context(), 90*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, "docker", append(append([]string{}, s.base...), args...)...)
	c.Dir = s.root
	// Compose credentials come from the runner's private file, never inherited
	// user settings or application/test DSNs.
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if strings.HasPrefix(k, "COMPOSE_") || strings.HasPrefix(k, "FIXTURE_") || strings.HasPrefix(k, "POSTGRES_") || strings.HasPrefix(k, "EXTRACTOR_") || strings.HasPrefix(k, "TRANSFORMER_") || strings.HasPrefix(k, "LOADER_") {
			continue
		}
		c.Env = append(c.Env, v)
	}
	c.WaitDelay = 5 * time.Second
	return c.CombinedOutput()
}

// compose invokes the existing fixture Compose definition for the current isolated project and
// fails on command error.
func (s *system) compose(args ...string) []byte {
	s.t.Helper()
	out, err := s.command(args...)
	if err != nil {
		s.t.Fatalf("Compose %s failed: %v\n%s", args[0], err, out)
	}
	return out
}

// until polls a scenario assertion within its deadline and fails explicitly instead of silently
// skipping unavailable work.
func (s *system) until(label string, check func() bool) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), 45*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if check() {
			s.t.Log("PASS:", label)
			return
		}
		select {
		case <-ctx.Done():
			s.t.Fatal("timeout:", label)
		case <-tick.C:
		}
	}
}

// is reads an integer PostgreSQL assertion and compares it with the expected count during bounded
// polling.
func (s *system) is(p *pgxpool.Pool, q string, want int) bool {
	ctx, cancel := context.WithTimeout(s.t.Context(), 2*time.Second)
	defer cancel()
	var n int
	return p.QueryRow(ctx, q).Scan(&n) == nil && n == want
}

// require requires an owning-schema query to match the expected durable state; query failure
// cannot count as success.
func (s *system) require(p *pgxpool.Pool, q string, want int) {
	s.t.Helper()
	if !s.is(p, q, want) {
		s.t.Fatalf("assertion failed (expected %d): %s", want, q)
	}
}

// control sends a local synthetic RPC/fault-control update; no real Ethereum node is involved.
func (s *system) control(body string) {
	s.t.Helper()
	req, err := http.NewRequestWithContext(s.t.Context(), "POST", s.url+"/control", strings.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	r, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		s.t.Fatal("fixture control unavailable")
	}
	defer r.Body.Close()
	if r.StatusCode != 204 {
		s.t.Fatal("fixture control rejected")
	}
}

// state reads local fixture control/ACK counters for scenario assertions.
func (s *system) state() fixture.State {
	s.t.Helper()
	req, _ := http.NewRequestWithContext(s.t.Context(), "GET", s.url+"/control", nil)
	r, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		s.t.Fatal("fixture status unavailable")
	}
	defer r.Body.Close()
	var state fixture.State
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&state) != nil {
		s.t.Fatal("invalid fixture status")
	}
	return state
}

// drained requires all participating durable queues to reach the expected completed/delivered
// count.
func (s *system) drained(n int) bool {
	return s.is(s.ex, "SELECT count(*) FROM extractor.outbox_events WHERE status='delivered'", n) && s.is(s.tr, "SELECT count(*) FROM transformer.inbox_events WHERE status='completed'", n) && s.is(s.tr, "SELECT count(*) FROM transformer.outbox_events WHERE status='delivered'", n) && s.is(s.lo, "SELECT count(*) FROM loader.inbox_events WHERE status='completed'", n) && s.is(s.lo, "SELECT count(*) FROM loader.blocks", n)
}

// probe queries a local service health route for the disposable fixture environment.
func (s *system) probe(port int, path string) bool {
	_, err := s.command("exec", "-T", "fixture", "/app/probe", fmt.Sprintf("http://127.0.0.1:%d/%s", port, path))
	return err == nil
}

// stop stops the selected test-owned services/processes without deleting unrelated storage.
func (s *system) stop(services ...string) {
	s.compose(append([]string{"stop", "-t", "20"}, services...)...)
	for _, name := range services {
		id := strings.TrimSpace(string(s.compose("ps", "-a", "-q", name)))
		ctx, cancel := context.WithTimeout(s.t.Context(), 5*time.Second)
		out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.ExitCode}}", id).CombinedOutput()
		cancel()
		if err != nil || strings.TrimSpace(string(out)) != "0" {
			s.t.Fatalf("%s did not exit gracefully: %s", name, out)
		}
	}
}

// pool opens an explicitly configured owning-role test pool with test cleanup; it is never a
// cross-service runtime dependency.
func pool(t *testing.T, service string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv(strings.ToUpper(service) + "_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid test database setting")
	}
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	cfg.MaxConns = 2
	p, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("cannot open test database")
	}
	t.Cleanup(p.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var role string
	if p.QueryRow(ctx, "SELECT current_user").Scan(&role) != nil || role != service+"_runtime" {
		t.Fatal("incorrect runtime role")
	}
	return p
}

// TestFixtureE2E runs the isolated real-service synthetic pipeline through outages, lost ACKs,
// restarts, replay and reorg assertions.
func TestFixtureE2E(t *testing.T) {
	if os.Getenv("FIXTURE_E2E_ISOLATED") != "1" {
		t.Skip("run go run ./test/integration/cmd/fixture-e2e")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	project, file := os.Getenv("FIXTURE_E2E_PROJECT"), os.Getenv("FIXTURE_E2E_ENV_FILE")
	if !strings.HasPrefix(project, "ethereum-stage11-") || file == "" {
		t.Fatal("isolated Compose project required")
	}
	s := &system{t: t, root: root, url: os.Getenv("FIXTURE_E2E_URL"), ex: pool(t, "extractor"), tr: pool(t, "transformer"), lo: pool(t, "loader")}
	s.base = []string{"compose", "--env-file", file, "-p", project, "-f", filepath.Join(root, "deployments/compose/compose.yaml"), "-f", filepath.Join(root, "deployments/compose/fixture.yaml"), "--profile", "fixture"}
	// No raw pre-seeding: every row must originate at the HTTP RPC fixture.
	s.require(s.ex, "SELECT count(*) FROM extractor.raw_blocks", 0)
	s.until("all services ready", func() bool { return s.probe(8091, "ready") && s.probe(8092, "ready") && s.probe(8093, "ready") })
	s.stop("transformer", "loader")
	s.control(`{"head":102,"drop_raw":true,"drop_transformed":true}`)
	s.until("Extractor retains two events while Transformer is offline", func() bool {
		return s.is(s.ex, "SELECT count(*) FROM extractor.raw_blocks", 2) && s.is(s.ex, "SELECT count(*) FROM extractor.outbox_events WHERE attempts>0 AND last_error_code='unavailable'", 2)
	})
	if !s.probe(8091, "ready") {
		t.Fatal("Extractor readiness depends on downstream availability")
	}
	s.require(s.ex, "SELECT block_number::int FROM extractor.extraction_checkpoints", 101)
	m := s.metrics(8091)
	if m[`etl_queue_backlog{queue="outbox"}`] != 2 || m["etl_extractor_checkpoint"] != 101 || m["etl_extractor_lag_blocks"] != 1 {
		t.Fatal("extractor backlog/head metrics", m)
	}
	s.require(s.ex, "SELECT count(*) FROM extractor.raw_blocks WHERE block_number=102", 0)
	s.compose("start", "transformer")
	s.until("Transformer retains output while Loader is offline", func() bool {
		return s.is(s.tr, "SELECT count(*) FROM transformer.inbox_events WHERE status='completed'", 2) && s.is(s.tr, "SELECT count(*) FROM transformer.outbox_events WHERE attempts>0 AND last_error_code='unavailable'", 2)
	})
	if !s.probe(8092, "ready") {
		t.Fatal("Transformer readiness depends on Loader availability")
	}
	transforming := s.metrics(8092)
	if transforming[`etl_queue_backlog{queue="outbox"}`] != 2 || transforming[`etl_queue_events{queue="inbox",state="done"}`] != 2 {
		t.Fatal("transformer queue metrics", transforming)
	}
	s.compose("start", "loader")
	s.until("durable handoff resumes after both outages", func() bool { return s.drained(2) })
	for _, port := range []int{8091, 8092} {
		m := s.metrics(port)
		if m[`etl_operation_retries_total{operation="dispatch_outbox"}`] < 1 {
			t.Fatal("missing retry metric", port)
		}
	}
	state := s.state()
	if state.RawLost != 1 || state.TransformedLost != 1 || state.RawDuplicates < 1 || state.TransformedDuplicates < 1 || state.Changed {
		t.Fatalf("lost ACK/deduplication evidence: %+v", state)
	}
	t.Logf("PASS: lost ACK after durable acceptance on both hops; duplicate ACKs raw=%d transformed=%d", state.RawDuplicates, state.TransformedDuplicates)
	// Keep processes alive during a real database outage, then let pools reconnect.
	s.stop("postgres")
	s.until("database outage removes readiness", func() bool { return !s.probe(8091, "ready") && !s.probe(8092, "ready") && !s.probe(8093, "ready") })
	for _, port := range []int{8091, 8092, 8093} {
		if !s.probe(port, "live") {
			t.Fatal("service exited during database outage")
		}
	}
	for _, port := range []int{8091, 8092, 8093} {
		m := s.metrics(port)
		if m["etl_database_snapshot_success"] != 0 {
			t.Fatal("DB outage reported success", port)
		}
		for name := range m {
			if strings.HasPrefix(name, "etl_queue_") || name == "etl_extractor_checkpoint" || name == "etl_extractor_revision" || name == "etl_extractor_lag_blocks" {
				t.Fatal("DB outage returned stale or zero gauge", name)
			}
		}
	}
	s.control(`{"head":104}`)
	s.compose("start", "postgres")
	s.until("database recovery and follow-head continuation", func() bool {
		return s.drained(4) && s.probe(8091, "ready") && s.probe(8092, "ready") && s.probe(8093, "ready")
	})
	s.require(s.ex, "SELECT block_number::int FROM extractor.extraction_checkpoints", 103)
	before := s.history()
	priorStart := s.metrics(8093)["etl_process_start_time_seconds"]
	s.stop("extractor", "transformer", "loader")
	s.compose("start", "extractor", "transformer", "loader")
	s.until("service restart preserves durable cursor and data", func() bool {
		return s.drained(4) && s.probe(8091, "ready") && s.probe(8092, "ready") && s.probe(8093, "ready")
	})
	if !reflect.DeepEqual(before, s.history()) {
		t.Fatal("service restart changed original history")
	}
	restarted := s.metrics(8093)
	if restarted["etl_process_start_time_seconds"] <= priorStart || restarted[`etl_queue_events{queue="inbox",state="done"}`] != 4 || restarted[`etl_operations_total{operation="process_inbox",outcome="completed"}`] != 0 {
		t.Fatal("restart metrics do not separate process and durable state", restarted)
	}
	// Use the real Stage 10 operator command; an accepted Inbox still deduplicates.
	event := "raw-block/v1/1/100/" + strings.TrimPrefix(fixture.Hash(100, false), "0x")
	replayLog := s.compose("run", "--rm", "--no-deps", "-e", "EXTRACTOR_MODE=replay", "-e", "EXTRACTOR_OPERATION_ID=stage11-replay", "-e", "EXTRACTOR_REPLAY_EVENT_ID="+event, "extractor")
	if !bytes.Contains(replayLog, []byte(`"operation":"replay_outbox"`)) || !bytes.Contains(replayLog, []byte(`"event_id":"`+event+`"`)) || !bytes.Contains(replayLog, []byte(`"outcome":"requeued"`)) {
		t.Fatal("replay correlation missing")
	}
	s.until("operator replay delivers without duplicate materialization", func() bool { return s.drained(4) && s.state().RawDuplicates > state.RawDuplicates })
	s.require(s.ex, "SELECT count(*) FROM extractor.replays", 1)
	if !reflect.DeepEqual(before, s.history()) {
		t.Fatal("replay changed immutable history")
	}
	s.control(`{"head":105,"fork":true}`)
	s.until("simulated reorg recovers and retains both branches", func() bool {
		return s.drained(7) && s.is(s.ex, "SELECT revision::int FROM extractor.extraction_checkpoints", 1)
	})
	s.require(s.ex, "SELECT block_number::int FROM extractor.extraction_checkpoints", 104)
	s.require(s.ex, "SELECT count(*) FROM extractor.active_blocks", 5)
	s.require(s.ex, "SELECT count(*) FROM extractor.rewinds WHERE from_number=103 AND to_number=101 AND previous_revision=0", 1)
	after := s.history()
	for id, digest := range before {
		if after[id] != digest {
			t.Fatal("reorg changed/lost historical observation", id)
		}
	}
	s.verifyRecords()
	for _, port := range []int{8091, 8092, 8093} {
		m := s.metrics(port)
		if m["etl_database_snapshot_success"] != 1 {
			t.Fatal("metrics failed after recovery", port)
		}
		for _, q := range []string{"inbox", "outbox"} {
			if _, ok := m[`etl_queue_backlog{queue="`+q+`"}`]; ok && m[`etl_queue_events{queue="`+q+`",state="done"}`] != 7 {
				t.Fatal("wrong durable completed metric", port, q)
			}
			if backlog, ok := m[`etl_queue_backlog{queue="`+q+`"}`]; ok && backlog != 0 {
				t.Fatal("nonempty final metrics backlog", port, q)
			}
		}
	}
	final := s.metrics(8091)
	if final["etl_extractor_checkpoint"] != 104 || final["etl_extractor_revision"] != 1 || final["etl_extractor_head"] != 105 || final["etl_extractor_lag_blocks"] != 1 {
		t.Fatal("reorg metrics", final)
	}
	loaded := s.metrics(8093)
	if loaded[`etl_queue_events{queue="inbox",state="done"}`] != 7 || loaded[`etl_operation_duration_seconds_count{operation="persist_block"}`] < 3 {
		t.Fatal("missing loader persistence metrics", loaded)
	}
	t.Log("PASS: metrics backlog, retry, outage without healthy zeros, restart semantics, persistence and reorg")
	s.stop("extractor", "transformer", "loader")
	s.verifyCorrelation(after)
	t.Log("PASS: full Compose fixture E2E, preservation, normalized data, graceful shutdown")
	if os.Getenv("ETL_STORAGE_VERIFY") == "1" {
		t.Run("storage_release", func(t *testing.T) {
			copy := *s
			copy.t = t
			copy.verifyStorageRelease()
		})
	}
}

// Include original source bytes, receipt bytes, provenance and stable event ID.
func (s *system) history() map[string][32]byte {
	s.t.Helper()
	rows, err := s.ex.Query(s.t.Context(), `SELECT o.event_id,b.raw_block,b.raw_receipts,b.provider,b.extracted_at FROM extractor.raw_blocks b JOIN extractor.outbox_events o USING(chain_id,block_number,block_hash) ORDER BY o.event_id`)
	if err != nil {
		s.t.Fatal("history query failed")
	}
	defer rows.Close()
	result := map[string][32]byte{}
	for rows.Next() {
		var id, provider, at string
		var raw []byte
		var receipts [][]byte
		if rows.Scan(&id, &raw, &receipts, &provider, &at) != nil {
			s.t.Fatal("history scan failed")
		}
		data, _ := json.Marshal([]any{id, raw, receipts, provider, at})
		result[id] = sha256.Sum256(data)
	}
	if rows.Err() != nil {
		s.t.Fatal("history read failed")
	}
	return result
}

// verifyRecords compares normalized records and retained branch observations after the full
// synthetic scenario.
func (s *system) verifyRecords() {
	s.t.Helper()
	for _, q := range []string{"SELECT count(*) FROM extractor.raw_blocks", "SELECT count(*) FROM extractor.outbox_events"} {
		s.require(s.ex, q, 7)
	}
	for _, q := range []string{"SELECT count(*) FROM transformer.inbox_events", "SELECT count(*) FROM transformer.transformed_blocks", "SELECT count(*) FROM transformer.outbox_events"} {
		s.require(s.tr, q, 7)
	}
	s.require(s.lo, "SELECT count(*) FROM loader.inbox_events", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.blocks WHERE canonical_status='unknown'", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.blocks WHERE height_conflict", 4)
	s.require(s.lo, "SELECT count(*) FROM loader.transactions", 14)
	s.require(s.lo, "SELECT count(*) FROM loader.receipts", 14)
	s.require(s.lo, "SELECT count(*) FROM loader.logs", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.transactions WHERE transaction_index=0 AND value=9007199254740993", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.receipts WHERE transaction_index=0 AND status=1 AND gas_used=25000", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.receipts WHERE transaction_index=1 AND status=0 AND cumulative_gas_used=46000 AND to_address IS NULL", 7)
	s.require(s.lo, "SELECT count(*) FROM loader.logs WHERE encode(data,'hex')='000102' AND removed=false", 7)
	for _, branch := range []struct {
		fork       bool
		start, end uint64
	}{{false, 100, 103}, {true, 102, 104}} {
		for n := branch.start; n <= branch.end; n++ {
			hash := strings.TrimPrefix(fixture.Hash(n, branch.fork), "0x")
			expected, expectedReceipts := fixture.Block(n, branch.fork, true)
			var raw []byte
			var receipts [][]byte
			var event, at, provider string
			err := s.ex.QueryRow(s.t.Context(), `SELECT b.raw_block,b.raw_receipts,o.event_id,b.extracted_at,b.provider FROM extractor.raw_blocks b JOIN extractor.outbox_events o USING(chain_id,block_number,block_hash) WHERE b.block_number=$1 AND encode(b.block_hash,'hex')=$2`, n, hash).Scan(&raw, &receipts, &event, &at, &provider)
			if err != nil || !bytes.Equal(raw, expected) || len(receipts) != 2 || provider != "stage11-fixture" || event != fmt.Sprintf("raw-block/v1/1/%d/%s", n, hash) {
				s.t.Fatalf("incorrect raw snapshot %d", n)
			}
			for i := range receipts {
				if !bytes.Equal(receipts[i], expectedReceipts[i]) {
					s.t.Fatal("raw receipt changed")
				}
			}
			var received []byte
			var receivedReceipts [][]byte
			var receivedAt, receivedProvider string
			err = s.tr.QueryRow(s.t.Context(), `SELECT raw_block,raw_receipts,extracted_at,provider FROM transformer.inbox_events WHERE event_id=$1`, event).Scan(&received, &receivedReceipts, &receivedAt, &receivedProvider)
			if err != nil || !bytes.Equal(received, raw) || !reflect.DeepEqual(receivedReceipts, receipts) || receivedAt != at || receivedProvider != provider {
				s.t.Fatal("raw handoff changed content/provenance")
			}
			var transformed, loaded []byte
			var transformedID string
			err = s.tr.QueryRow(s.t.Context(), `SELECT event_id,payload FROM transformer.transformed_blocks WHERE source_event_id=$1`, event).Scan(&transformedID, &transformed)
			if err != nil {
				s.t.Fatal("missing transformation")
			}
			err = s.lo.QueryRow(s.t.Context(), `SELECT payload FROM loader.inbox_events WHERE event_id=$1 AND source_event_id=$2`, transformedID, event).Scan(&loaded)
			if err != nil || !bytes.Equal(transformed, loaded) {
				s.t.Fatal("transformed handoff changed content")
			}
			var timestamp string
			var parent string
			err = s.lo.QueryRow(s.t.Context(), `SELECT timestamp::text,encode(parent_hash,'hex') FROM loader.blocks WHERE block_number=$1 AND encode(block_hash,'hex')=$2`, n, hash).Scan(&timestamp, &parent)
			if err != nil || timestamp != fmt.Sprint(1700000000+n) || parent != strings.TrimPrefix(fixture.Hash(n-1, branch.fork), "0x") {
				s.t.Fatalf("incorrect normalized block %d: timestamp=%s parent=%s error=%v", n, timestamp, parent, err)
			}
			if branch.fork || n < 102 {
				var active string
				err = s.ex.QueryRow(s.t.Context(), `SELECT encode(block_hash,'hex') FROM extractor.active_blocks WHERE block_number=$1`, n).Scan(&active)
				if err != nil || active != hash {
					s.t.Fatal("incorrect active branch")
				}
			}
		}
	}
	if s.state().Changed {
		s.t.Fatal("a duplicate event changed its original content")
	}
	// Explicitly confirm that service-role isolation also holds in this deployment.
	for _, pair := range []struct {
		p       *pgxpool.Pool
		foreign string
	}{{s.ex, "transformer.inbox_events"}, {s.tr, "loader.blocks"}, {s.lo, "extractor.raw_blocks"}} {
		if _, err := pair.p.Exec(s.t.Context(), "SELECT 1 FROM "+pair.foreign+" LIMIT 1"); err == nil {
			s.t.Fatal("cross-service access allowed")
		}
	}
}

// Inspect actual service stdout/stderr after graceful shutdown, not synthetic logs.
func (s *system) verifyCorrelation(history map[string][32]byte) {
	s.t.Helper()
	logs := s.compose("logs", "--no-color", "--no-log-prefix", "extractor", "transformer", "loader")
	seen := map[string]bool{}
	for _, line := range bytes.Split(logs, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r map[string]any
		if err := json.Unmarshal(line, &r); err != nil {
			s.t.Fatal("service log is not structured JSON")
		}
		for _, key := range []string{"service", "operation", "event_id", "source_event_id", "chain_id", "block_number", "block_hash", "attempt", "outcome", "error_code", "duration_ms"} {
			if _, ok := r[key]; !ok {
				s.t.Fatal("missing log field", key)
			}
		}
		if _, ok := r["error"]; ok {
			s.t.Fatal("raw error field in service log")
		}
		id, _ := r["event_id"].(string)
		if strings.HasPrefix(id, "transformed-block/") && r["source_event_id"] != strings.Replace(id, "transformed-block/", "raw-block/", 1) {
			s.t.Fatal("transformed source correlation missing")
		}
		if id != "" && (r["chain_id"] != "1" || r["block_hash"] == nil || r["block_number"] == nil) {
			s.t.Fatal("block correlation missing")
		}
		key := fmt.Sprint(r["service"], "/", r["operation"], "/", r["outcome"], "/", id)
		seen[key] = true
	}
	for raw := range history {
		transformed := strings.Replace(raw, "raw-block/", "transformed-block/", 1)
		for _, key := range []string{"extractor-service/index_block/completed/" + raw, "extractor-service/dispatch_outbox/delivered/" + raw, "transformer-service/process_inbox/completed/" + raw, "transformer-service/dispatch_outbox/delivered/" + transformed, "loader-service/process_inbox/completed/" + transformed} {
			if !seen[key] {
				s.t.Fatal("missing end-to-end correlated log", key)
			}
		}
	}
	for _, prefix := range []string{"extractor-service/dispatch_outbox/retry/", "transformer-service/dispatch_outbox/retry/", "transformer-service/accept_inbox/already_accepted/", "loader-service/accept_inbox/already_accepted/", "extractor-service/reorg_recovery/rewound/", "extractor-service/rewind_checkpoint/rewound/"} {
		found := false
		for key := range seen {
			if strings.HasPrefix(key, prefix) {
				found = true
				break
			}
		}
		if !found {
			s.t.Fatal("missing fault/recovery log", prefix)
		}
	}
	s.t.Log("PASS: structured correlation across both hops, retries, duplicate ACK, replay and reorg")
}

// metrics reads and parses a local service metrics body for fixture assertions.
func (s *system) metrics(port int) map[string]float64 {
	s.t.Helper()
	body := s.compose("exec", "-T", "fixture", "/app/probe", "--body", fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	result := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			s.t.Fatal("invalid metric exposition")
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			s.t.Fatal("invalid sample value")
		}
		if _, exists := result[fields[0]]; exists {
			s.t.Fatal("duplicate metric series")
		}
		result[fields[0]] = value
		for _, forbidden := range []string{"event_id=", "block_hash=", "block_number=", "error=", "chain_id=", "provider="} {
			if strings.Contains(fields[0], forbidden) {
				s.t.Fatal("unbounded metric label")
			}
		}
	}
	if _, ok := result["etl_database_snapshot_success"]; !ok {
		s.t.Fatal("missing snapshot status")
	}
	return result
}
