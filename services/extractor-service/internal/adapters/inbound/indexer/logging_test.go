// logging_test.go exercises event correlation and secret/error redaction on success and failure in
// the inbound adapter layer.

package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// TestIndexLogsCommittedAndFailedBlocksAtShutdown checks cancellation does not erase committed or
// failed block correlation from progress logs.
func TestIndexLogsCommittedAndFailedBlocksAtShutdown(t *testing.T) {
	first := domain.BlockID{ChainID: "1", Number: 10, Hash: domain.Hash{1}}
	second := domain.BlockID{ChainID: "1", Number: 11, Hash: domain.Hash{2}}
	for _, reindex := range []bool{false, true} {
		var output bytes.Buffer
		ctx, cancel := context.WithCancel(t.Context())
		w, _ := New(cycleFunc(func(context.Context) (inbound.IndexResult, error) {
			cancel()
			return inbound.IndexResult{Completed: []inbound.IndexedBlock{{Block: first}}, Block: &second, Reindex: reindex}, errors.Join(outbound.ErrCommitUnknown, errors.New("postgres://secret/raw-payload"))
		}), slog.New(slog.NewJSONHandler(&output, nil)), time.Second, time.Second, 1)
		if err := w.Run(ctx); err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
		if len(lines) != 2 {
			t.Fatal("lost batch progress", output.String())
		}
		var success, failure map[string]any
		if err := json.Unmarshal(lines[0], &success); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(lines[1], &failure); err != nil {
			t.Fatal(err)
		}
		if success["event_id"] != first.EventID() || success["outcome"] != "completed" || failure["event_id"] != second.EventID() || failure["outcome"] != "unknown" || failure["error_code"] != "commit_unknown" {
			t.Fatal(success, failure)
		}
		if reindex && failure["operation"] != "reindex_block" {
			t.Fatal(failure)
		}
		if strings.Contains(output.String(), "secret") {
			t.Fatal("error leaked")
		}
	}
}

// TestRecoveryLogsOldTipAndAncestorWithoutClaimingSuccess checks recovery progress retains both
// identities and does not report an uncommitted rewind as success.
func TestRecoveryLogsOldTipAndAncestorWithoutClaimingSuccess(t *testing.T) {
	tip := domain.BlockID{ChainID: "1", Number: 13, Hash: domain.Hash{3}}
	ancestor := domain.BlockID{ChainID: "1", Number: 11, Hash: domain.Hash{1}}
	for _, failure := range []error{nil, outbound.ErrCommitUnknown, domain.ErrRecoveryRequired} {
		var output bytes.Buffer
		writeProgress(t.Context(), slog.New(slog.NewJSONHandler(&output, nil)), inbound.IndexResult{Block: &tip, Ancestor: &ancestor, Recovering: true, Rewound: failure == nil}, failure, time.Now())
		lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
		if len(lines) != 2 {
			t.Fatal(output.String())
		}
		for i, line := range lines {
			var r map[string]any
			if err := json.Unmarshal(line, &r); err != nil {
				t.Fatal(err)
			}
			want := tip.EventID()
			if i == 0 {
				want = ancestor.EventID()
			}
			if r["event_id"] != want {
				t.Fatal(r)
			}
			if failure != nil && r["outcome"] == "rewound" {
				t.Fatal("claimed successful rewind", r)
			}
		}
	}
}
