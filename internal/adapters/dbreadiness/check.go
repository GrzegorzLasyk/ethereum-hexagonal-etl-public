// check.go implements bounded read-only schema/migration/privilege checks for readiness in the
// shared technical support layer, outside service domains.

// Package dbreadiness checks a service-owned PostgreSQL contract without writes.
// Policies are adapter-owned technical manifests, not shared domain models.
package dbreadiness

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

// ErrNotReady signals service database is not ready. Callers can match this category with
// errors.Is without parsing error text.
var ErrNotReady = errors.New("service database is not ready")

// Database is the minimal transaction-opening dependency needed by owning-schema readiness checks.
type Database interface {
	// BeginTx starts an adapter-owned database transaction with the supplied options; the caller owns
	// completion/rollback.
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// Table describes one required owning-schema relation and its expected runtime privileges.
type Table struct {
	Name                    string
	Columns, Types, Updates []string
	Insert, Delete          bool
	View                    bool
}

// Policy declares required owning-schema objects, migrations and privileges for readiness rather
// than mutating the schema.
type Policy struct {
	Schema     string
	Tables     []Table
	Indexes    []string
	Migrations map[string]string
}

// Check returns only a fixed error. The caller never receives SQL, server or
// connection diagnostics. No application data or other service schema is read.
func Check(ctx context.Context, db Database, p Policy) error {
	call, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	tx, err := db.BeginTx(call, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ErrNotReady
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(call), 100*time.Millisecond)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	if err := check(call, tx, p); err != nil {
		return ErrNotReady
	}
	if call.Err() != nil {
		return ErrNotReady
	}
	return nil
}

// check checks required relations, metadata and privilege grants within the read-only transaction;
// failure remains a safe not-ready result.
func check(ctx context.Context, tx pgx.Tx, p Policy) error {
	var valid bool
	err := tx.QueryRow(ctx, `SELECT current_database()='ethereum_etl' AND current_user=$1 AND session_user=$1
 AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls OR r.rolinherit)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid)
 AND pg_catalog.has_database_privilege(current_user,current_database(),'CONNECT')
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE,TEMP')
 AND n.nspowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$3)
 AND pg_catalog.has_schema_privilege(current_user,n.oid,'USAGE')
 AND NOT pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE')
 FROM pg_catalog.pg_roles r JOIN pg_catalog.pg_namespace n ON n.nspname=$2 WHERE r.rolname=current_user`, p.Schema+"_runtime", p.Schema, p.Schema+"_migrator").Scan(&valid)
	if err != nil || !valid {
		return ErrNotReady
	}
	for _, table := range p.Tables {
		var oid uint32
		kind := "r"
		if table.View {
			kind = "v"
		}
		err = tx.QueryRow(ctx, `SELECT c.oid,c.relkind::text=$3 AND NOT c.relrowsecurity
 AND pg_catalog.pg_get_userbyid(c.relowner)=$4
 AND pg_catalog.has_table_privilege(current_user,c.oid,'SELECT')
 AND pg_catalog.has_table_privilege(current_user,c.oid,'INSERT')=$5
 AND pg_catalog.has_table_privilege(current_user,c.oid,'DELETE')=$6
 AND NOT pg_catalog.has_table_privilege(current_user,c.oid,'TRUNCATE,REFERENCES,TRIGGER')
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2`, p.Schema, table.Name, kind, p.Schema+"_migrator", table.Insert, table.Delete).Scan(&oid, &valid)
		if err != nil || !valid {
			return ErrNotReady
		}
		err = tx.QueryRow(ctx, `SELECT count(*)=cardinality($2::text[]) AND COALESCE(bool_and(
 a.attnum IS NOT NULL AND pg_catalog.format_type(a.atttypid,a.atttypmod)=expected.typ
 AND pg_catalog.has_column_privilege(current_user,$1::oid,a.attnum,'UPDATE')=(expected.name=ANY($4::text[]))
 AND pg_catalog.has_column_privilege(current_user,$1::oid,a.attnum,'INSERT')=$5
 ),false)
 FROM unnest($2::text[],$3::text[]) expected(name,typ)
 LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=$1::oid AND a.attname=expected.name AND a.attnum>0 AND NOT a.attisdropped`, oid, table.Columns, table.Types, nonNil(table.Updates), table.Insert).Scan(&valid)
		if err != nil || !valid {
			return ErrNotReady
		}
	}
	for _, name := range p.Indexes {
		err = tx.QueryRow(ctx, `SELECT i.indisvalid AND i.indisready FROM pg_catalog.pg_index i
 JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2`, p.Schema, name).Scan(&valid)
		if err != nil || !valid {
			return ErrNotReady
		}
	}
	// The owning migrator exposes only version/checksum through this narrow view.
	rows, err := tx.Query(ctx, "SELECT version,checksum FROM "+pgx.Identifier{p.Schema, "schema_readiness"}.Sanitize())
	if err != nil {
		return ErrNotReady
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var version, checksum string
		if rows.Scan(&version, &checksum) != nil {
			return ErrNotReady
		}
		expected, known := p.Migrations[version]
		if seen[version] || !known || expected != checksum {
			return ErrNotReady
		}
		seen[version] = true
	}
	if rows.Err() != nil || len(seen) != len(p.Migrations) {
		return ErrNotReady
	}
	return nil
}

// nonNil normalizes a nil slice to an empty SQL-array argument so readiness membership checks have
// defined semantics.
func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
