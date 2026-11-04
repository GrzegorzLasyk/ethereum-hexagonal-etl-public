// logging_test.go exercises event correlation and secret/error redaction on success and failure in
// the inbound adapter layer.

package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
)

// TestCorrelatedProgressSurvivesCancellationAndErrors checks that a claimed event's
// identity/attempt remains available in failure and shutdown logs.
func TestCorrelatedProgressSurvivesCancellationAndErrors(t *testing.T) {
	raw := "raw-block/v1/1/42/" + strings.Repeat("a", 64)
	id, source := raw, ""
	id, source = strings.Replace(raw, "raw-block", "transformed-block", 1), raw
	for _, tc := range []struct {
		name, outcome, code, want string
		err                       error
	}{
		{name: "success after cancellation", outcome: "completed", want: "completed"},
		{name: "lost ACK or transient failure", outcome: "retry", code: "unavailable", want: "retry"},
		{name: "permanent failure", outcome: "failed", code: "invalid", want: "failed"},
		{name: "ambiguous commit", err: errors.Join(outbound.ErrCommitUnknown, errors.New("postgres://secret:password@host/raw-receipts")), code: "commit_unknown", want: "unknown"},
		{name: "unknown error redacted", err: errors.New("postgres://secret:password@host/raw-receipts"), code: "operation_failed", want: "interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var output bytes.Buffer
			p := processorFunc(func(context.Context) (inbound.ProcessResult, error) {
				cancel()
				return inbound.ProcessResult{Found: true, EventID: id, SourceEventID: source, Attempt: 3, Outcome: tc.outcome, ErrorCode: tc.code}, tc.err
			})
			w, err := worker.New(p, slog.New(slog.NewJSONHandler(&output, nil)), 1, time.Hour, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			w.Run(ctx)
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err, output.String())
			}
			for key, want := range map[string]any{"service": "loader-service", "operation": "process_inbox", "event_id": id, "chain_id": "1", "block_number": float64(42), "block_hash": "0x" + strings.Repeat("a", 64), "attempt": float64(3), "outcome": tc.want, "error_code": tc.code} {
				if record[key] != want {
					t.Errorf("%s=%v want %v", key, record[key], want)
				}
			}
			if source != "" && record["source_event_id"] != source {
				t.Fatal("source correlation lost", record)
			}
			if _, ok := record["duration_ms"].(float64); !ok {
				t.Fatal("missing duration")
			}
			if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "raw-receipts") {
				t.Fatal("sensitive error logged")
			}
		})
	}
}

// TestClaimFailureDoesNotInventIdentity requires failure before a claim to omit event identity
// rather than log a fabricated event.
func TestClaimFailureDoesNotInventIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var output bytes.Buffer
	w, _ := worker.New(processorFunc(func(context.Context) (inbound.ProcessResult, error) {
		cancel()
		return inbound.ProcessResult{}, outbound.ErrCommitUnknown
	}), slog.New(slog.NewJSONHandler(&output, nil)), 1, time.Hour, time.Second)
	w.Run(ctx)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["event_id"] != nil || record["attempt"] != nil || record["outcome"] != "unknown" {
		t.Fatal(record)
	}
}
