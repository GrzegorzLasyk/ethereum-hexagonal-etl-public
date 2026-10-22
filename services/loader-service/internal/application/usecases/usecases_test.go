package usecases

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

type fakeStore struct {
	item                                      *outbound.Item
	duplicate                                 bool
	accepted                                  int
	acceptErr, claimErr, completeErr, failErr error
	completed                                 *domain.Document
	failure                                   *outbound.Failure
	afterAccept, afterComplete                func()
}

func (s *fakeStore) Accept(context.Context, inbound.AcceptTransformedBlockRequest) (bool, error) {
	s.accepted++
	if s.afterAccept != nil {
		s.afterAccept()
	}
	return s.duplicate, s.acceptErr
}
func (s *fakeStore) Claim(context.Context) (*outbound.Item, error) { return s.item, s.claimErr }
func (s *fakeStore) Complete(_ context.Context, _ outbound.Item, d domain.Document) error {
	s.completed = &d
	if s.afterComplete != nil {
		s.afterComplete()
	}
	return s.completeErr
}
func (s *fakeStore) Fail(_ context.Context, _ outbound.Item, f outbound.Failure) error {
	s.failure = &f
	return s.failErr
}
func event(t *testing.T) inbound.AcceptTransformedBlockRequest {
	t.Helper()
	b, e := os.ReadFile("../../adapters/outbound/jsondocument/testdata/transformed_v1.json")
	if e != nil {
		t.Fatal(e)
	}
	var hash, parent [32]byte
	for i := range hash {
		hash[i] = 0x10
		parent[i] = 0x11
	}
	return inbound.AcceptTransformedBlockRequest{EventID: fmt.Sprintf("transformed-block/v1/1/20000000/%x", hash), SourceEventID: fmt.Sprintf("raw-block/v1/1/20000000/%x", hash), EventVersion: 1, PayloadVersion: 1, Block: inbound.BlockIdentity{ChainID: "1", Number: 20000000, Hash: hash, ParentHash: parent}, Provider: "fixture", ExtractedAt: time.Unix(100, 123), TransformedAt: time.Unix(200, 456), PayloadJSON: b}
}
func processor(t *testing.T, s *fakeStore) *Process {
	t.Helper()
	p, e := NewProcess(s, jsondocument.Decoder{}, time.Millisecond, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestAcceptanceDurableOutcomes(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s := &fakeStore{duplicate: duplicate, afterAccept: cancel}
		u, _ := NewAccept("1", 64<<20, s)
		got, e := u.AcceptTransformedBlock(ctx, event(t))
		want := inbound.DurablyAccepted
		if duplicate {
			want = inbound.AlreadyAccepted
		}
		if e != nil || got.Acceptance != want || s.accepted != 1 {
			t.Fatal(got, e)
		}
	}
	for _, failure := range []error{inbound.ErrUnavailable, inbound.ErrConflict, outbound.ErrCommitUnknown, context.DeadlineExceeded} {
		s := &fakeStore{acceptErr: failure}
		u, _ := NewAccept("1", 64<<20, s)
		got, e := u.AcceptTransformedBlock(t.Context(), event(t))
		if got.Acceptance != inbound.AcceptanceUnspecified || !errors.Is(e, failure) {
			t.Fatal(got, e)
		}
	}
}
func TestEnvelopeRejectedBeforeIO(t *testing.T) {
	for _, change := range []func(*inbound.AcceptTransformedBlockRequest){
		func(r *inbound.AcceptTransformedBlockRequest) { r.EventID = "bad" }, func(r *inbound.AcceptTransformedBlockRequest) { r.SourceEventID = "bad" },
		func(r *inbound.AcceptTransformedBlockRequest) { r.PayloadVersion = 2 }, func(r *inbound.AcceptTransformedBlockRequest) { r.Block.ChainID = "2" },
		func(r *inbound.AcceptTransformedBlockRequest) { r.TransformedAt = time.Time{} }, func(r *inbound.AcceptTransformedBlockRequest) { r.PayloadJSON = nil },
		func(r *inbound.AcceptTransformedBlockRequest) { r.ExtractedAt = time.Time{} }, func(r *inbound.AcceptTransformedBlockRequest) { r.Provider = "https://secret" },
	} {
		r := event(t)
		change(&r)
		s := &fakeStore{}
		u, _ := NewAccept("1", 64<<20, s)
		if _, e := u.AcceptTransformedBlock(t.Context(), r); e == nil || s.accepted != 0 {
			t.Fatal("invalid envelope persisted")
		}
	}
	s := &fakeStore{}
	u, _ := NewAccept("1", 1, s)
	if _, e := u.AcceptTransformedBlock(t.Context(), event(t)); e == nil {
		t.Fatal("oversize accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e := u.AcceptTransformedBlock(ctx, event(t)); !errors.Is(e, context.Canceled) || s.accepted != 0 {
		t.Fatal(e)
	}
}
func TestProcessingAndProvenanceBinding(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*inbound.AcceptTransformedBlockRequest)
		code   string
	}{
		{"valid", func(*inbound.AcceptTransformedBlockRequest) {}, ""},
		{"syntax", func(r *inbound.AcceptTransformedBlockRequest) { r.PayloadJSON = []byte("{") }, "invalid"},
		{"missing receipts", func(r *inbound.AcceptTransformedBlockRequest) {
			r.PayloadJSON = []byte(`{"schema_version":1,"chain_id":"1","source_event_id":"x","provider":"fixture","extracted_at":"1970-01-01T00:01:40Z","block":{}}`)
		}, "invalid"},
		{"identity", func(r *inbound.AcceptTransformedBlockRequest) { r.Block.Hash[0]++ }, "identity"},
		{"provider", func(r *inbound.AcceptTransformedBlockRequest) { r.Provider = "other" }, "identity"},
		{"time", func(r *inbound.AcceptTransformedBlockRequest) { r.ExtractedAt = r.ExtractedAt.Add(time.Nanosecond) }, "identity"},
		{"source event", func(r *inbound.AcceptTransformedBlockRequest) { r.SourceEventID = "other" }, "identity"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := event(t)
			tt.change(&r)
			s := &fakeStore{item: &outbound.Item{Event: r, Attempt: 1, MaxAttempts: 3}}
			got, e := processor(t, s).ProcessOne(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			if tt.code == "" {
				if got.Outcome != "completed" || s.completed == nil {
					t.Fatal(got)
				}
			} else if got.Outcome != "failed" || s.completed != nil || s.failure == nil || s.failure.Code != tt.code {
				t.Fatal(got, s.failure)
			}
		})
	}
}
func TestRetryCancellationAndAmbiguousCommit(t *testing.T) {
	for _, failure := range []error{inbound.ErrUnavailable, inbound.ErrConflict, outbound.ErrCommitUnknown, outbound.ErrLeaseLost} {
		s := &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 2, MaxAttempts: 3}, completeErr: failure}
		got, e := processor(t, s).ProcessOne(t.Context())
		if errors.Is(failure, outbound.ErrCommitUnknown) || errors.Is(failure, outbound.ErrLeaseLost) {
			if !errors.Is(e, failure) || s.failure != nil {
				t.Fatal("ambiguous/stale state overwritten", e)
			}
			continue
		}
		want := "retry"
		if failure == inbound.ErrConflict {
			want = "failed"
		}
		if e != nil || got.Outcome != want || s.failure == nil || s.failure.Delay < time.Millisecond || s.failure.Delay > 2*time.Millisecond {
			t.Fatal(got, e)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	s := &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 1, MaxAttempts: 3}, afterComplete: cancel, completeErr: inbound.ErrUnavailable}
	if _, e := processor(t, s).ProcessOne(ctx); !errors.Is(e, context.Canceled) || s.failure != nil {
		t.Fatal("cancelled work not left for lease recovery", e)
	}
	s = &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 3, MaxAttempts: 3}, completeErr: inbound.ErrUnavailable}
	if got, e := processor(t, s).ProcessOne(t.Context()); e != nil || got.Outcome != "failed" {
		t.Fatal(got, e)
	}
	s = &fakeStore{}
	if got, e := processor(t, s).ProcessOne(t.Context()); e != nil || got.Found {
		t.Fatal(got, e)
	}
	s.claimErr = inbound.ErrUnavailable
	if _, e := processor(t, s).ProcessOne(t.Context()); !errors.Is(e, inbound.ErrUnavailable) {
		t.Fatal(e)
	}
	s = &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 1, MaxAttempts: 3}, completeErr: inbound.ErrUnavailable, failErr: outbound.ErrLeaseLost}
	if _, e := processor(t, s).ProcessOne(t.Context()); !errors.Is(e, outbound.ErrLeaseLost) {
		t.Fatal(e)
	}
	p := processor(t, s)
	if d := p.backoff(1000); d < time.Second/2 || d > time.Second {
		t.Fatal(d)
	}
}
