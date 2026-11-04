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
		{"missing migration view", "ALTER VIEW transformer.schema_readiness RENAME TO readiness_probe", "ALTER VIEW transformer.readiness_probe RENAME TO schema_readiness"},
		{"missing table", "ALTER TABLE transformer.inbox_events RENAME TO readiness_probe", "ALTER TABLE transformer.readiness_probe RENAME TO inbox_events"},
		{"missing column", "ALTER TABLE transformer.inbox_events RENAME COLUMN raw_block TO readiness_probe", "ALTER TABLE transformer.inbox_events RENAME COLUMN readiness_probe TO raw_block"},
		{"wrong column type", "ALTER TABLE transformer.inbox_events ALTER COLUMN max_attempts TYPE bigint", "ALTER TABLE transformer.inbox_events ALTER COLUMN max_attempts TYPE integer"},
		{"missing index", "ALTER INDEX transformer.inbox_pending RENAME TO readiness_probe", "ALTER INDEX transformer.readiness_probe RENAME TO inbox_pending"},
		{"missing migration", "UPDATE transformer.schema_migrations SET version='9999_readiness_probe.sql' WHERE version='0001_schema_privileges.sql'", "UPDATE transformer.schema_migrations SET version='0001_schema_privileges.sql' WHERE version='9999_readiness_probe.sql'"},
		{"checksum mismatch", "UPDATE transformer.schema_migrations SET checksum=repeat('0',64) WHERE version='0001_schema_privileges.sql'", "UPDATE transformer.schema_migrations SET checksum='ea47b832d2853912dc8d45b6dc43b647b6053a5722d49b8b5ece9e4b1e52f14b' WHERE version='0001_schema_privileges.sql'"},
		{"missing select", "REVOKE SELECT ON transformer.inbox_events FROM transformer_runtime", "GRANT SELECT ON transformer.inbox_events TO transformer_runtime"},
		{"missing insert", "REVOKE INSERT ON transformer.inbox_events FROM transformer_runtime", "GRANT INSERT ON transformer.inbox_events TO transformer_runtime"},
		{"missing update", "REVOKE UPDATE(status) ON transformer.inbox_events FROM transformer_runtime", "GRANT UPDATE(status) ON transformer.inbox_events TO transformer_runtime"},
		{"immutable update granted", "GRANT UPDATE(raw_block) ON transformer.inbox_events TO transformer_runtime", "REVOKE UPDATE(raw_block) ON transformer.inbox_events FROM transformer_runtime"},
		{"delete granted", "GRANT DELETE ON transformer.inbox_events TO transformer_runtime", "REVOKE DELETE ON transformer.inbox_events FROM transformer_runtime"},
		{"metadata unavailable", "REVOKE SELECT ON transformer.schema_readiness FROM transformer_runtime", "GRANT SELECT ON transformer.schema_readiness TO transformer_runtime"},
		{"metadata writable", "GRANT INSERT ON transformer.schema_readiness TO transformer_runtime", "REVOKE INSERT ON transformer.schema_readiness FROM transformer_runtime"},
		{"schema usage revoked", "REVOKE USAGE ON SCHEMA transformer FROM transformer_runtime", "GRANT USAGE ON SCHEMA transformer TO transformer_runtime"},
		{"schema create granted", "GRANT CREATE ON SCHEMA transformer TO transformer_runtime", "REVOKE CREATE ON SCHEMA transformer FROM transformer_runtime"},
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
		if _, err = tx.Exec(t.Context(), "LOCK TABLE transformer.schema_migrations IN ACCESS EXCLUSIVE MODE"); err != nil {
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
