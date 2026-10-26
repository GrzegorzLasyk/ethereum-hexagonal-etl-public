package logevent

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type secretError struct{}

func (secretError) Error() string { panic("must never format underlying error") }

func TestRedactionAndExistingIdentity(t *testing.T) {
	raw := "raw-block/v1/1/0/" + strings.Repeat("a", 64)
	transformed := strings.Replace(raw, "raw-block", "transformed-block", 1)
	for _, tc := range []struct {
		id, source    string
		valid, linked bool
	}{
		{raw, "", true, false}, {transformed, raw, true, true},
		{transformed, "postgres://secret", true, false},
		{transformed, strings.Replace(raw, "/1/0/", "/1/1/", 1), true, false},
		{"postgres://secret", raw, false, false},
		{"raw-block/v1/01/0/" + strings.Repeat("a", 64), "", false, false},
		{"raw-block/v1/1/18446744073709551616/" + strings.Repeat("a", 64), "", false, false},
		{"raw-block/v1/1/0/" + strings.Repeat("0", 64), "", false, false},
	} {
		var output bytes.Buffer
		Write(t.Context(), slog.New(slog.NewJSONHandler(&output, nil)), Event{Service: "extractor-service", Operation: "replay_outbox", EventID: tc.id, SourceEventID: tc.source, Outcome: "interrupted", ErrorCode: ErrorCode(secretError{})}, time.Now())
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if (record["event_id"] != nil) != tc.valid || (record["source_event_id"] != nil) != tc.linked {
			t.Fatal(record)
		}
		if tc.valid && record["block_number"] != float64(0) {
			t.Fatal("genesis identity lost", record)
		}
		if strings.Contains(output.String(), "secret") {
			t.Fatal("untrusted metadata leaked")
		}
		for _, key := range []string{"service", "operation", "event_id", "source_event_id", "chain_id", "block_number", "block_hash", "attempt", "outcome", "error_code", "duration_ms"} {
			if _, ok := record[key]; !ok {
				t.Fatal("missing field", key)
			}
		}
	}
}
