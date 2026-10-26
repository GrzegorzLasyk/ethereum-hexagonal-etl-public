package postgres

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReadinessMigrationManifest(t *testing.T) {
	files, err := filepath.Glob("../../../../../../migrations/extractor/*.sql")
	if err != nil || len(files) != len(readinessPolicy.Migrations) {
		t.Fatal("readiness manifest must track migrations", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if readinessPolicy.Migrations[filepath.Base(file)] != fmt.Sprintf("%x", sha256.Sum256(data)) {
			t.Fatal("readiness checksum mismatch", file)
		}
	}
	for _, table := range readinessPolicy.Tables {
		if len(table.Columns) == 0 || len(table.Columns) != len(table.Types) {
			t.Fatal("incomplete table contract", table.Name)
		}
	}
}
