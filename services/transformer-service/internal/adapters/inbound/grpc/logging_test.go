package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
)

func TestAcceptanceLogsDurableIdentityAndRedactsFailures(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, code string
		acceptance          inbound.Acceptance
		err                 error
	}{
		{name: "durable", outcome: "accepted", acceptance: inbound.DurablyAccepted},
		{name: "duplicate after lost ACK", outcome: "already_accepted", acceptance: inbound.AlreadyAccepted},
		{name: "ambiguous acceptance", outcome: "unknown", code: "commit_unknown", err: errors.Join(outbound.ErrCommitUnknown, errors.New("postgres://secret:password@host"))},
		{name: "failure", outcome: "interrupted", code: "operation_failed", err: errors.New("payload-and-receipts-secret")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			req := request()
			req.Metadata.Provider = "secret-provider"
			req.Payload.BlockJson = []byte(`{"secret-block":"raw-payload"}`)
			req.Payload.Receipts.Json = [][]byte{[]byte(`{"secret-receipt":"raw-receipt"}`)}
			r, err := NewReceiver("1", grpccontract.DefaultLimits(), acceptFunc(func(context.Context, inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
				return inbound.AcceptanceResult{EventID: req.Metadata.EventId, Acceptance: tc.acceptance}, tc.err
			}), slog.New(slog.NewJSONHandler(&output, nil)))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = r.SubmitRawBlock(t.Context(), req)
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["event_id"] != req.Metadata.EventId || record["outcome"] != tc.outcome || record["error_code"] != tc.code {
				t.Fatal(record)
			}

			if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "opaque") {
				t.Fatal("data leaked", output.String())
			}
		})
	}
	var output bytes.Buffer
	req := request()
	req.Metadata.EventId = "postgres://secret"
	r, _ := NewReceiver("1", grpccontract.DefaultLimits(), nil, slog.New(slog.NewJSONHandler(&output, nil)))
	_, _ = r.SubmitRawBlock(t.Context(), req)
	if strings.Contains(output.String(), "secret") {
		t.Fatal("invalid metadata leaked")
	}
}
