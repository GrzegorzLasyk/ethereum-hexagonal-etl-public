//go:build composeintegration

package fixture_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// This is an administrative disaster-recovery test, never an application adapter.
// It accepts only the freshly generated fixture project, retains both clusters,
// and refuses to restore over any existing application database.
func (s *system) pg(service, role, database string, args ...string) ([]byte, error) {
	key := strings.ToUpper(role) + "_PASSWORD"
	if role == "postgres" {
		key = "POSTGRES_PASSWORD"
	}
	cmd := []string{"exec", "-T", service, "bash", "-ceu", `export PGPASSWORD="${!1}"; shift; exec "$@"`, "storage-check", key}
	return s.command(append(cmd, args...)...)
}

func (s *system) sql(service, role, database, query string) []byte {
	s.t.Helper()
	out, err := s.pg(service, role, database, "psql", "-X", "-w", "-h", "127.0.0.1", "-U", role, "-d", database, "-v", "ON_ERROR_STOP=1", "-At", "-c", query)
	if err != nil {
		s.t.Fatalf("storage SQL failed (%s/%s): %s", service, role, out)
	}
	return bytes.TrimSpace(out)
}

func (s *system) admin(service string, args ...string) {
	s.t.Helper()
	if out, err := s.pg(service, "postgres", "postgres", args...); err != nil {
		s.t.Fatalf("storage tool failed: %s", out)
	}
}

func (s *system) verifyStorageRelease() {
	s.base = append(s.base, "-f", filepath.Join(s.root, "deployments/compose/storage.yaml"))
	dir, err := os.MkdirTemp("", "ethereum-storage-evidence-")
	if err != nil {
		s.t.Fatal(err)
	}
	// Intentionally retained, including on failure; never put a real backup in Git.
	s.t.Log("Retained private evidence directory:", dir)
	snapshot := s.storageSnapshot("postgres")
	started := time.Now()
	s.admin("postgres", "pg_dump", "-w", "-h", "127.0.0.1", "-U", "postgres", "-Fc", "--file=/tmp/etl-snapshot.dump", "ethereum_etl")
	dumpDuration := time.Since(started)
	s.compose("cp", "postgres:/tmp/etl-snapshot.dump", filepath.Join(dir, "snapshot.dump"))
	dump, err := os.ReadFile(filepath.Join(dir, "snapshot.dump"))
	if err != nil {
		s.t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, "snapshot.dump"), 0600); err != nil {
		s.t.Fatal(err)
	}
	digest := sha256.Sum256(dump)
	writeEvidence(s, dir, "snapshot.sha256", []byte(fmt.Sprintf("%x  snapshot.dump\n", digest)))
	s.compose("up", "-d", "--wait", "--wait-timeout", "120", "restore")
	if !s.restoreEmpty() {
		s.t.Fatal("refusing restore: application database already exists")
	}
	// Roles are cluster-global and are not in pg_dump. Recreate only the six
	// approved roles with private, generated credentials; no password dump.
	var roles strings.Builder
	roles.WriteString("\\set ON_ERROR_STOP on\nBEGIN;\n")
	for _, service := range []string{"extractor", "transformer", "loader"} {
		for _, kind := range []string{"migrator", "runtime"} {
			role := service + "_" + kind
			fmt.Fprintf(&roles, "\\getenv password %s_PASSWORD\nCREATE ROLE %s LOGIN PASSWORD :'password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;\n", strings.ToUpper(role), role)
		}
	}
	roles.WriteString("COMMIT;\n")
	writeEvidence(s, dir, "roles.sql", []byte(roles.String()))
	s.compose("cp", filepath.Join(dir, "roles.sql"), "restore:/tmp/etl-roles.sql")
	s.admin("restore", "psql", "-X", "-w", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-f", "/tmp/etl-roles.sql")
	s.compose("cp", filepath.Join(dir, "snapshot.dump"), "restore:/tmp/etl-snapshot.dump")
	// Verify the copied archive before executing it; no --clean/--no-owner/--no-acl.
	copied := s.compose("exec", "-T", "restore", "sha256sum", "/tmp/etl-snapshot.dump")
	if !strings.HasPrefix(string(copied), fmt.Sprintf("%x ", digest)) {
		s.t.Fatal("archive checksum mismatch")
	}
	started = time.Now()
	if err := s.restoreArchive(); err != nil {
		s.t.Fatal(err)
	}
	restoreDuration := time.Since(started)
	if err := s.restoreArchive(); err == nil {
		s.t.Fatal("populated restore destination accepted")
	}
	if got := s.storageSnapshot("restore"); !bytes.Equal(snapshot, got) {
		s.t.Fatal("restore differs: complete rows, metadata or privileges")
	}
	writeEvidence(s, dir, "restored-manifest.txt", snapshot)
	for _, service := range []string{"extractor", "transformer", "loader"} {
		role := service + "_runtime"
		got := s.sql("restore", role, "ethereum_etl", "SELECT current_user; SELECT count(*) FROM "+service+".schema_readiness")
		if !strings.HasPrefix(string(got), role+"\n") {
			s.t.Fatal("restored runtime authentication failed")
		}
		for _, foreign := range []string{"extractor", "transformer", "loader"} {
			if foreign == service {
				continue
			}
			_, err := s.pg("restore", role, "ethereum_etl", "psql", "-X", "-w", "-h", "127.0.0.1", "-U", role, "-d", "ethereum_etl", "-v", "ON_ERROR_STOP=1", "-c", "SELECT * FROM "+foreign+".schema_readiness")
			if err == nil {
				s.t.Fatal("restored role can access foreign schema")
			}
		}
	}
	s.t.Logf("PASS: backup/restore bytes=%d sha256=%x dump=%s restore=%s; all table rows and schema/ACL metadata identical; populated target rejected", len(dump), digest, dumpDuration, restoreDuration)
	s.measureStorage(dir)
	if !bytes.Equal(snapshot, s.storageSnapshot("postgres")) {
		s.t.Fatal("measurement changed original fixture database")
	}
	s.t.Log("PASS: storage release verification; original history preserved; both volumes retained")
}

func (s *system) restoreEmpty() bool {
	return string(s.sql("restore", "postgres", "postgres", "SELECT count(*) FROM pg_database WHERE datname='ethereum_etl'")) == "0"
}

func writeEvidence(s *system, dir, name string, data []byte) {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		s.t.Fatal(err)
	}
}

func (s *system) storageSnapshot(service string) []byte {
	var result bytes.Buffer
	for _, schema := range []string{"extractor", "transformer", "loader"} {
		tables := s.sql(service, schema+"_migrator", "ethereum_etl", "SELECT tablename FROM pg_tables WHERE schemaname='"+schema+"' ORDER BY tablename")
		for _, table := range strings.Fields(string(tables)) {
			// Names come only from this test's own migrations, never external input.
			rows := s.sql(service, schema+"_migrator", "ethereum_etl", "SELECT row_to_json(t)::text FROM "+schema+"."+table+" t ORDER BY row_to_json(t)::text")
			fmt.Fprintf(&result, "%s.%s %x\n", schema, table, sha256.Sum256(rows))
		}
	}
	// Schema-only dump includes owners, indexes, constraints, column/table ACLs,
	// default privileges, views, DB ACLs and per-role database configuration.
	ddl, err := s.pg(service, "postgres", "ethereum_etl", "pg_dump", "-w", "-h", "127.0.0.1", "-U", "postgres", "--schema-only", "--create", "ethereum_etl")
	if err != nil {
		s.t.Fatal("schema snapshot failed")
	}
	var normalized strings.Builder
	for _, line := range strings.Split(string(ddl), "\n") {
		if strings.HasPrefix(line, "\\restrict ") || strings.HasPrefix(line, "\\unrestrict ") {
			continue
		}
		normalized.WriteString(line + "\n")
	}
	fmt.Fprintf(&result, "schema %x\n", sha256.Sum256([]byte(normalized.String())))
	roles := s.sql(service, "postgres", "ethereum_etl", "SELECT rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls FROM pg_roles WHERE rolname ~ '^(extractor|transformer|loader)_' ORDER BY rolname")
	fmt.Fprintf(&result, "roles %x\n", sha256.Sum256(roles))
	return result.Bytes()
}

// SQL is read from the current adapters rather than maintaining a second copy
// of their critical statements. Known parameter values are fixture constants.
func (s *system) queryLiteral(relative, contains string) string {
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(s.root, relative), nil, 0)
	if err != nil {
		s.t.Fatal(err)
	}
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			v, err := strconv.Unquote(lit.Value)
			if err == nil && strings.Contains(v, contains) {
				found = append(found, v)
			}
		}
		return true
	})
	if len(found) != 1 {
		s.t.Fatalf("query selector %q matched %d statements", contains, len(found))
	}
	return found[0]
}

func (s *system) measureStorage(dir string) {
	// SQL-shape fixtures only: copied payloads intentionally do not describe the
	// synthetic identities. Never start ETL workers against this measurement clone.
	for _, schema := range []string{"extractor", "transformer", "loader"} {
		tables := map[string][]string{"extractor": {"raw_blocks", "outbox_events"}, "transformer": {"inbox_events", "transformed_blocks", "outbox_events"}, "loader": {"inbox_events", "blocks"}}[schema]
		for _, table := range tables {
			prefix := "transformed-block"
			if schema == "extractor" || schema == "transformer" && table == "inbox_events" {
				prefix = "raw-block"
			}
			patch := fmt.Sprintf(`jsonb_build_object('chain_id','777','block_number',g,'block_hash','\x'||md5(g::text)||md5(g::text),'event_id','%s/v1/777/'||g||'/'||md5(g::text)||md5(g::text),'source_event_id','raw-block/v1/777/'||g||'/'||md5(g::text)||md5(g::text))`, prefix)
			if table == "inbox_events" || table == "outbox_events" {
				done, inflight, at, completed := "completed", "processing", "accepted_at", "completed_at"
				if table == "outbox_events" {
					done, inflight, at, completed = "delivered", "sending", "created_at", "delivered_at"
				}
				patch += fmt.Sprintf(` || jsonb_build_object('status',CASE WHEN g<=9900 THEN '%s' WHEN g<=9950 THEN 'pending' WHEN g<=9975 THEN '%s' ELSE 'failed' END,'attempts',1,'max_attempts',1000,'lease_until',CASE WHEN g>9950 AND g<=9975 THEN '2020-01-01T00:00:00Z' ELSE NULL END,'%s',CASE WHEN g<=9900 THEN '2020-01-01T00:00:00Z' ELSE NULL END,'%s','2020-01-01T00:00:00Z','next_attempt_at','2020-01-01T00:00:00Z')`, done, inflight, completed, at)
			}
			q := fmt.Sprintf(`INSERT INTO %s.%s SELECT (jsonb_populate_record(NULL::%s.%s,to_jsonb(t)||%s)).* FROM (SELECT * FROM %s.%s LIMIT 1) t CROSS JOIN generate_series(1,10000) g`, schema, table, schema, table, patch, schema, table)
			s.sql("restore", schema+"_migrator", "ethereum_etl", "SET statement_timeout='30s'; "+q+"; ANALYZE "+schema+"."+table)
		}
	}
	writeEvidence(s, dir, "environment.txt", s.sql("restore", "postgres", "ethereum_etl", "SELECT version(); SHOW shared_buffers; SHOW work_mem; SELECT pg_database_size(current_database()); SELECT schemaname,relname,n_live_tup,pg_total_relation_size(relid) FROM pg_stat_user_tables ORDER BY schemaname,relname"))
	type query struct{ name, service, file, match string }
	queries := []query{
		{"extractor_claim", "extractor", "outbox.go", "WITH candidate AS"},
		{"transformer_claim", "transformer", "outbox.go", "WITH candidate AS"},
		{"transformer_inbox", "transformer", "store.go", "WITH candidate AS"},
		{"loader_inbox", "loader", "store.go", "WITH candidate AS"},
		{"extractor_metrics", "extractor", "status.go", "SELECT count(*) FILTER"},
		{"extractor_capacity", "extractor", "branch.go", "SELECT count(*)::bigint,COALESCE(sum"},
		{"transformer_inbox_metrics", "transformer", "status.go", "FROM transformer.inbox_events e WHERE"},
		{"transformer_outbox_metrics", "transformer", "status.go", "FROM transformer.outbox_events e JOIN"},
		{"loader_metrics", "loader", "status.go", "SELECT count(*) FILTER"},
	}
	for _, q := range queries {
		sql := s.queryLiteral("services/"+q.service+"-service/internal/adapters/outbound/postgres/"+q.file, q.match)
		if strings.HasSuffix(sql, "RETURNING ") {
			sql += "event_id"
		}
		sql = strings.NewReplacer("$3", "1000", "$2", "60000000", "$1", "'777'").Replace(sql)
		s.explain(dir, q.name, q.service, sql)
	}
	s.explain(dir, "loader_neighbors", "loader", "SELECT block_number,block_hash,parent_hash FROM loader.blocks WHERE chain_id='777' AND block_number BETWEEN 4999 AND 5001")
	s.explain(dir, "loader_height", "loader", "SELECT block_hash,canonical_status,height_conflict,parent_link FROM loader.blocks WHERE chain_id='777' AND block_number=5000")
}

func (s *system) explain(dir, name, service, query string) {
	// EXPLAIN ANALYZE executes DML. Roll back each sample, even on this clone.
	for sample := 0; sample < 3; sample++ {
		out := s.sql("restore", service+"_runtime", "ethereum_etl", "BEGIN; SET LOCAL statement_timeout='15s'; EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query+"; ROLLBACK")
		start, end := bytes.IndexByte(out, '['), bytes.LastIndexByte(out, ']')
		if start < 0 || end < start {
			s.t.Fatal("missing EXPLAIN JSON")
		}
		plan := out[start : end+1]
		var parsed []map[string]any
		if json.Unmarshal(plan, &parsed) != nil || len(parsed) != 1 {
			s.t.Fatal("invalid EXPLAIN JSON")
		}
		writeEvidence(s, dir, fmt.Sprintf("%s-%d.json", name, sample), plan)
		s.t.Logf("MEASURE %s sample=%d execution_ms=%v planning_ms=%v", name, sample, parsed[0]["Execution Time"], parsed[0]["Planning Time"])
	}
}

func (s *system) restoreArchive() error {
	if !s.restoreEmpty() {
		return errors.New("refusing restore: destination database exists")
	}
	_, err := s.pg("restore", "postgres", "postgres", "pg_restore", "-w", "-h", "127.0.0.1", "-U", "postgres", "--exit-on-error", "--create", "-d", "postgres", "/tmp/etl-snapshot.dump")
	if err != nil {
		return errors.New("restore failed; destination retained for investigation")
	}
	return nil
}
