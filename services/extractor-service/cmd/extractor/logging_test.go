package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/config"
)

func TestReplayStartupFailureKeepsIdentityWithoutDSN(t *testing.T) {
	var output bytes.Buffer
	id := "raw-block/v1/1/42/" + strings.Repeat("a", 64)
	cfg := config.Config{DatabaseURL: "postgres://secret:%zz@host", Indexing: config.IndexingConfig{Mode: "replay", Replay: outbound.ReplayRequest{EventID: id}}}
	if err := run(t.Context(), cfg, slog.New(slog.NewJSONHandler(&output, nil))); err == nil {
		t.Fatal("invalid database URL accepted")
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["operation"] != "replay_outbox" || record["event_id"] != id || record["outcome"] != "interrupted" || record["error_code"] != "operation_failed" {
		t.Fatal(record)
	}
	if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "postgres:") {
		t.Fatal("DSN leaked")
	}
}
