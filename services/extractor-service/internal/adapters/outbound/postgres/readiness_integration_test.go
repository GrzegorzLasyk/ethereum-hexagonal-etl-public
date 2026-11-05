//go:build postgresintegration

// readiness_integration_test.go exercises schema, privilege, timeout and recovery readiness
// failures in PostgreSQL in the outbound adapter layer.

package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/health"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"testing"
	"time"
)

// testReadiness mutates only disposable schema/privilege state to verify not-ready, timeout and
// recovery behavior.
func testReadiness(t *testing.T, runtime, owner *pgxpool.Pool) {
	s, err := New(runtime, "1", 0)
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
		{"missing migration view", "ALTER VIEW extractor.schema_readiness RENAME TO readiness_probe", "ALTER VIEW extractor.readiness_probe RENAME TO schema_readiness"},
		{"missing table", "ALTER TABLE extractor.raw_blocks RENAME TO readiness_probe", "ALTER TABLE extractor.readiness_probe RENAME TO raw_blocks"},
		{"missing column", "ALTER TABLE extractor.raw_blocks RENAME COLUMN raw_block TO readiness_probe", "ALTER TABLE extractor.raw_blocks RENAME COLUMN readiness_probe TO raw_block"},
		{"wrong column type", "ALTER TABLE extractor.outbox_events ALTER COLUMN max_attempts TYPE bigint", "ALTER TABLE extractor.outbox_events ALTER COLUMN max_attempts TYPE integer"},
		{"missing index", "ALTER INDEX extractor.outbox_pending RENAME TO readiness_probe", "ALTER INDEX extractor.readiness_probe RENAME TO outbox_pending"},
		{"missing migration", "UPDATE extractor.schema_migrations SET version='9999_readiness_probe.sql' WHERE version='0001_schema_privileges.sql'", "UPDATE extractor.schema_migrations SET version='0001_schema_privileges.sql' WHERE version='9999_readiness_probe.sql'"},
		{"checksum mismatch", "UPDATE extractor.schema_migrations SET checksum=repeat('0',64) WHERE version='0001_schema_privileges.sql'", "UPDATE extractor.schema_migrations SET checksum='5df63707fb2eb55d41267f93756f5039e110927a98025db5761af882bedde7c2' WHERE version='0001_schema_privileges.sql'"},
		{"missing select", "REVOKE SELECT ON extractor.raw_blocks FROM extractor_runtime", "GRANT SELECT ON extractor.raw_blocks TO extractor_runtime"},
		{"missing insert", "REVOKE INSERT ON extractor.raw_blocks FROM extractor_runtime", "GRANT INSERT ON extractor.raw_blocks TO extractor_runtime"},
		{"missing update", "REVOKE UPDATE(status) ON extractor.outbox_events FROM extractor_runtime", "GRANT UPDATE(status) ON extractor.outbox_events TO extractor_runtime"},
		{"immutable update granted", "GRANT UPDATE(raw_block) ON extractor.raw_blocks TO extractor_runtime", "REVOKE UPDATE(raw_block) ON extractor.raw_blocks FROM extractor_runtime"},
		{"delete granted", "GRANT DELETE ON extractor.raw_blocks TO extractor_runtime", "REVOKE DELETE ON extractor.raw_blocks FROM extractor_runtime"},
		{"metadata unavailable", "REVOKE SELECT ON extractor.schema_readiness FROM extractor_runtime", "GRANT SELECT ON extractor.schema_readiness TO extractor_runtime"},
		{"metadata writable", "GRANT INSERT ON extractor.schema_readiness TO extractor_runtime", "REVOKE INSERT ON extractor.schema_readiness FROM extractor_runtime"},
		{"schema usage revoked", "REVOKE USAGE ON SCHEMA extractor FROM extractor_runtime", "GRANT USAGE ON SCHEMA extractor TO extractor_runtime"},
		{"schema create granted", "GRANT CREATE ON SCHEMA extractor TO extractor_runtime", "REVOKE CREATE ON SCHEMA extractor FROM extractor_runtime"},
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
		if _, err = tx.Exec(t.Context(), "LOCK TABLE extractor.schema_migrations IN ACCESS EXCLUSIVE MODE"); err != nil {
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
	wrong, err := New(owner, "1", 0)
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
