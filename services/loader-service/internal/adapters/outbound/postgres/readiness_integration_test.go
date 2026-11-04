//go:build postgresintegration

// readiness_integration_test.go exercises schema, privilege, timeout and recovery readiness
// failures in PostgreSQL in the outbound adapter layer.

package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/health"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"testing"
	"time"
)

// testReadiness mutates only disposable schema/privilege state to verify not-ready, timeout and
// recovery behavior.
func testReadiness(t *testing.T, runtime, owner *pgxpool.Pool) {
	s, err := New(runtime, "1", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	h := health.Handler(t.Context(), s.CheckReadiness)
	probe := func(want int) {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))
		if w.Code != want || w.Body.Len() != 0 {
			t.Fatalf("readiness=%d body length=%d want=%d", w.Code, w.Body.Len(), want)
		}
	}
	probe(200)
	for _, tc := range []struct{ name, breakSQL, restoreSQL string }{
		{"missing migration view", "ALTER VIEW loader.schema_readiness RENAME TO readiness_probe", "ALTER VIEW loader.readiness_probe RENAME TO schema_readiness"},
		{"missing table", "ALTER TABLE loader.inbox_events RENAME TO readiness_probe", "ALTER TABLE loader.readiness_probe RENAME TO inbox_events"},
		{"missing column", "ALTER TABLE loader.inbox_events RENAME COLUMN payload TO readiness_probe", "ALTER TABLE loader.inbox_events RENAME COLUMN readiness_probe TO payload"},
		{"wrong column type", "ALTER TABLE loader.inbox_events ALTER COLUMN max_attempts TYPE bigint", "ALTER TABLE loader.inbox_events ALTER COLUMN max_attempts TYPE integer"},
		{"missing index", "ALTER INDEX loader.inbox_pending RENAME TO readiness_probe", "ALTER INDEX loader.readiness_probe RENAME TO inbox_pending"},
		{"missing migration", "UPDATE loader.schema_migrations SET version='9999_readiness_probe.sql' WHERE version='0001_schema_privileges.sql'", "UPDATE loader.schema_migrations SET version='0001_schema_privileges.sql' WHERE version='9999_readiness_probe.sql'"},
		{"checksum mismatch", "UPDATE loader.schema_migrations SET checksum=repeat('0',64) WHERE version='0001_schema_privileges.sql'", "UPDATE loader.schema_migrations SET checksum='a9084dae29c21586355b0c71afc4bc73ae23de662a0a2f28c0ea1a8bb4151b6e' WHERE version='0001_schema_privileges.sql'"},
		{"missing select", "REVOKE SELECT ON loader.inbox_events FROM loader_runtime", "GRANT SELECT ON loader.inbox_events TO loader_runtime"},
		{"missing insert", "REVOKE INSERT ON loader.inbox_events FROM loader_runtime", "GRANT INSERT ON loader.inbox_events TO loader_runtime"},
		{"missing update", "REVOKE UPDATE(status) ON loader.inbox_events FROM loader_runtime", "GRANT UPDATE(status) ON loader.inbox_events TO loader_runtime"},
		{"immutable update granted", "GRANT UPDATE(payload) ON loader.inbox_events TO loader_runtime", "REVOKE UPDATE(payload) ON loader.inbox_events FROM loader_runtime"},
		{"delete granted", "GRANT DELETE ON loader.inbox_events TO loader_runtime", "REVOKE DELETE ON loader.inbox_events FROM loader_runtime"},
		{"metadata unavailable", "REVOKE SELECT ON loader.schema_readiness FROM loader_runtime", "GRANT SELECT ON loader.schema_readiness TO loader_runtime"},
		{"metadata writable", "GRANT INSERT ON loader.schema_readiness TO loader_runtime", "REVOKE INSERT ON loader.schema_readiness FROM loader_runtime"},
		{"schema usage revoked", "REVOKE USAGE ON SCHEMA loader FROM loader_runtime", "GRANT USAGE ON SCHEMA loader TO loader_runtime"},
		{"schema create granted", "GRANT CREATE ON SCHEMA loader TO loader_runtime", "REVOKE CREATE ON SCHEMA loader FROM loader_runtime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := owner.Exec(t.Context(), tc.breakSQL); err != nil {
				t.Fatal("fault injection", err)
			}
			defer func() {
				if _, err := owner.Exec(t.Context(), tc.restoreSQL); err != nil {
					t.Fatal("restore", err)
				}
				probe(200)
			}()
			probe(503)
		})
	}
	t.Run("deadline and recovery", func(t *testing.T) {
		tx, err := owner.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err = tx.Exec(t.Context(), "LOCK TABLE loader.schema_migrations IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := s.CheckReadiness(ctx); err == nil || time.Since(started) > 2*time.Second {
			t.Fatal("readiness did not honor deadline")
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		probe(200)
	})
	wrong, err := New(owner, "1", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.CheckReadiness(t.Context()); err == nil {
		t.Fatal("migrator treated as runtime")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.CheckReadiness(ctx); err == nil {
		t.Fatal("canceled readiness succeeded")
	}
}
