// batch_test.go exercises bounded pgx INSERT grouping and batch-result cleanup inside a block
// transaction in the startup configuration boundary.

package config_test

import (
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/config"
)

// TestSQLBatchSize checks direct mode, default and maximum batch settings and rejects unsupported
// values before startup.
func TestSQLBatchSize(t *testing.T) {
	setDefaults(t)
	for _, c := range []struct {
		value string
		want  int
	}{{"", 64}, {"1", 1}, {"256", 256}, {"0", 0}, {"257", 0}, {"-1", 0}, {"bad", 0}} {
		t.Run(c.value, func(t *testing.T) {
			t.Setenv("LOADER_SQL_BATCH_SIZE", c.value)
			got, err := config.Load()
			if c.want == 0 {
				if err == nil {
					t.Fatal("invalid batch size accepted")
				}
				return
			}
			if err != nil || got.SQLBatchSize != c.want {
				t.Fatal(got.SQLBatchSize, err)
			}
		})
	}
}
