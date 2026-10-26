package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

func TestImportPolicyRejectsBoundaryViolations(t *testing.T) {
	own := module + "/services/extractor-service/internal/"
	for _, tc := range []struct {
		layer, imp string
		allowed    bool
	}{
		{"domain", "math/big", true}, {"application", "context", true},
		{"domain", own + "domain", true}, {"application", own + "domain", true},
		{"application", own + "application/ports/outbound", true},
		{"domain", own + "application/ports/outbound", false},
		{"domain", "database/sql", false}, {"application", "database/sql/driver", false},
		{"application", "github.com/jackc/pgx/v5", false},
		{"domain", "net/http", false}, {"application", "net/rpc", false},
		{"application", "google.golang.org/grpc", false},
		{"domain", "google.golang.org/protobuf/proto", false},
		{"application", "github.com/segmentio/kafka-go", false},
		{"application", "github.com/rabbitmq/amqp091-go", false},
		{"domain", "os", false}, {"application", "os/exec", false},
		{"application", own + "adapters/outbound/postgres", false},
		{"application", own + "config", false},
		{"domain", module + "/gen/etl/v1", false},
		{"application", module + "/internal/adapters/health", false},
		{"application", module + "/internal/innocenthelper", false},
		{"domain", own + "domainlookalike", false},
		{"adapters", module + "/services/loader-service/internal/domain", false},
		{"cmd", module + "/services/transformer-service/internal/application/ports/inbound", false},
		{"domain", module + "/services/loader-service/internal/domain", false},
		{"adapters", "github.com/jackc/pgx/v5", true},
		{"cmd", own + "adapters/outbound/postgres", true},
	} {
		t.Run(tc.layer+"/"+tc.imp, func(t *testing.T) {
			if got := importViolation("extractor-service", tc.layer, tc.imp); (got == "") != tc.allowed {
				t.Fatalf("allowed=%v, violation=%q", tc.allowed, got)
			}
		})
	}
}

func TestSchemaPolicy(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		allowed bool
	}{
		{"SELECT * FROM extractor.raw_blocks", true},
		{"SELECT pg_database_size(current_database())", true},
		{"-- loader.blocks is forbidden\nSELECT 1", true},
		{"/* transformer.inbox_events */ SELECT 1", true},
		{"SELECT * FROM loader.blocks", false},
		{"SELECT * FROM extractor.raw_blocks a JOIN transformer.inbox_events b ON true", false},
		{"INSERT INTO loader.blocks VALUES (1)", false},
		{"UPDATE transformer.inbox_events SET status='completed'", false},
		{"DELETE FROM loader.blocks", false},
		{"TRUNCATE TABLE loader.blocks", false},
		{"REFERENCES loader.blocks(id)", false},
		{"SELECT * FROM \"loader\" . \"blocks\"", false},
		{"SELECT * FROM LOADER.\nblocks", false},
		{"SELECT * FROM loader/* comment */.blocks", false},
		{"SELECT nextval('loader.sequence')", false},
		{"SELECT 'transformer.inbox_events'::regclass", false},
		{"SELECT loader.some_function()", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if got := foreignSchemas(tc.sql, "extractor"); (len(got) == 0) != tc.allowed {
				t.Fatalf("allowed=%v, schemas=%v", tc.allowed, got)
			}
		})
	}
}

func TestGoSchemaInspection(t *testing.T) {
	for _, tc := range []struct {
		body    string
		allowed bool
	}{
		{"const q = `SELECT * FROM loader.blocks`", false},
		{"const q = \"SELECT * FROM loader.\\nblocks\"", false},
		{"const q = \"SELECT * FROM loa\" + (\"der.\" + \"blocks\")", false},
		{"const q = \"SELECT * FROM \" + schema + \".blocks\"", false},
		{"func f() { _ = `SELECT * FROM transformer.inbox_events` }", false},
		{"// SELECT * FROM loader.blocks\nconst q = `SELECT * FROM extractor.raw_blocks`", true},
		{"const a = b; const b = a", true}, // malformed cycles must not hang the checker
	} {
		t.Run(tc.body, func(t *testing.T) {
			set := token.NewFileSet()
			var files []*ast.File
			for i, source := range []string{"package example\nconst schema = \"loader\"", "package example\n" + tc.body} {
				f, err := parser.ParseFile(set, strconv.Itoa(i)+".go", source, 0)
				if err != nil {
					t.Fatal(err)
				}
				files = append(files, f)
			}
			if got := goSchemaViolations(set, files, "extractor"); (len(got) == 0) != tc.allowed {
				t.Fatalf("allowed=%v, violations=%v", tc.allowed, got)
			}
		})
	}
}
