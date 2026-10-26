package usecases

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/domain"
)

// Fakes simulate port outcomes only; production durability is tested separately.
type fakeStore struct {
	item                                      *outbound.Item
	accepted                                  int
	duplicate                                 bool
	acceptErr, claimErr, completeErr, failErr error
	completed                                 *outbound.Result
	failure                                   *outbound.Failure
	afterAccept                               func()
}

func (s *fakeStore) Accept(ctx context.Context, r inbound.AcceptRawBlockRequest) (bool, error) {
	s.accepted++
	if s.afterAccept != nil {
		s.afterAccept()
	}
	return s.duplicate, s.acceptErr
}
func (s *fakeStore) Claim(context.Context) (*outbound.Item, error) { return s.item, s.claimErr }
func (s *fakeStore) Complete(_ context.Context, _ outbound.Item, r outbound.Result) error {
	s.completed = &r
	return s.completeErr
}
func (s *fakeStore) Fail(_ context.Context, _ outbound.Item, f outbound.Failure) error {
	s.failure = &f
	return s.failErr
}
func event(t *testing.T) inbound.AcceptRawBlockRequest {
	t.Helper()
	read := func(name string) []byte {
		b, e := os.ReadFile("../../adapters/outbound/ethereumjson/testdata/" + name)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	var hash, parent [32]byte
	for i := range hash {
		hash[i] = 0x10
		parent[i] = 0x11
	}
	return inbound.AcceptRawBlockRequest{EventID: fmt.Sprintf("raw-block/v1/1/20000000/%x", hash), EventVersion: 1, PayloadVersion: 1,
		Block: inbound.BlockIdentity{ChainID: "1", Number: 20000000, Hash: hash, ParentHash: parent}, Provider: "fixture", ExtractedAt: time.Unix(100, 123),
		RawBlock: read("block.json"), RawReceipts: [][]byte{read("receipt_0.json"), read("receipt_1.json")}}
}
func processor(t *testing.T, s *fakeStore) *Process {
	t.Helper()
	e, _ := jsondocument.New(64 << 20)
	p, err := NewProcess(s, ethereumjson.Decoder{}, e, time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestAcceptanceCommitAndFailures(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		s := &fakeStore{duplicate: duplicate, afterAccept: cancel}
		a, _ := NewAccept("1", 64<<20, s)
		got, err := a.AcceptRawBlock(ctx, event(t))
		cancel()
		want := inbound.DurablyAccepted
		if duplicate {
			want = inbound.AlreadyAccepted
		}
		if err != nil || got.Acceptance != want || s.accepted != 1 {
			t.Fatal(got, err)
		}
	}
	for _, failure := range []error{inbound.ErrConflict, inbound.ErrUnavailable, outbound.ErrCommitUnknown, context.DeadlineExceeded} {
		s := &fakeStore{acceptErr: failure}
		a, _ := NewAccept("1", 64<<20, s)
		got, err := a.AcceptRawBlock(t.Context(), event(t))
		if got.Acceptance != inbound.AcceptanceUnspecified || !errors.Is(err, failure) {
			t.Fatal(got, err)
		}
	}
}
func TestApplicationEnvelopeValidation(t *testing.T) {
	for _, change := range []func(*inbound.AcceptRawBlockRequest){
		func(r *inbound.AcceptRawBlockRequest) { r.EventID = "wrong" }, func(r *inbound.AcceptRawBlockRequest) { r.EventVersion = 2 },
		func(r *inbound.AcceptRawBlockRequest) { r.Block.ChainID = "2" }, func(r *inbound.AcceptRawBlockRequest) { r.RawReceipts[0] = nil },
		func(r *inbound.AcceptRawBlockRequest) { r.ExtractedAt = time.Time{} }, func(r *inbound.AcceptRawBlockRequest) { r.Provider = "https://secret" },
	} {
		s := &fakeStore{}
		a, _ := NewAccept("1", 64<<20, s)
		r := event(t)
		change(&r)
		if _, err := a.AcceptRawBlock(t.Context(), r); err == nil || s.accepted != 0 {
			t.Fatal("invalid event persisted")
		}
	}
	s := &fakeStore{}
	a, _ := NewAccept("1", 1, s)
	if _, err := a.AcceptRawBlock(t.Context(), event(t)); err == nil {
		t.Fatal("oversize accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.AcceptRawBlock(ctx, event(t)); !errors.Is(err, context.Canceled) || s.accepted != 0 {
		t.Fatal(err)
	}
}
func TestProcessSuccessAndPoison(t *testing.T) {
	for _, tt := range []struct {
		name          string
		change        func(*inbound.AcceptRawBlockRequest)
		outcome, code string
	}{
		{"valid", func(*inbound.AcceptRawBlockRequest) {}, "completed", ""},
		{"missing receipt", func(r *inbound.AcceptRawBlockRequest) { r.RawReceipts = r.RawReceipts[:1] }, "failed", "incomplete"},
		{"invalid syntax", func(r *inbound.AcceptRawBlockRequest) { r.RawBlock = []byte("{") }, "failed", "invalid"},
		{"wrong metadata", func(r *inbound.AcceptRawBlockRequest) { r.Block.Hash[0]++ }, "failed", "identity"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := event(t)
			tt.change(&r)
			s := &fakeStore{item: &outbound.Item{Event: r, Attempt: 1, MaxAttempts: 3}}
			p := processor(t, s)
			got, err := p.ProcessOne(t.Context())
			if err != nil || got.Outcome != tt.outcome {
				t.Fatal(got, err)
			}
			if tt.code != "" && (s.completed != nil || s.failure == nil || s.failure.Code != tt.code || !s.failure.Permanent) {
				t.Fatal("poison incorrectly processed")
			}
			if tt.code == "" && (s.completed == nil || s.completed.EventID != "transformed-block/v1/1/20000000/1010101010101010101010101010101010101010101010101010101010101010") {
				t.Fatal("missing output")
			}
		})
	}
}
func TestProcessingRetryAndAmbiguousCommit(t *testing.T) {
	for _, err := range []error{inbound.ErrUnavailable, outbound.ErrCommitUnknown, outbound.ErrLeaseLost} {
		s := &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 2, MaxAttempts: 3}, completeErr: err}
		result, got := processor(t, s).ProcessOne(t.Context())
		if result.EventID != s.item.Event.EventID || result.Attempt != s.item.Attempt {
			t.Fatalf("lost correlation: %+v", result)
		}
		if errors.Is(err, inbound.ErrUnavailable) {
			if got != nil || result.Outcome != "retry" || s.failure == nil || s.failure.Permanent {
				t.Fatal(result, got)
			}
		} else if !errors.Is(got, err) || s.failure != nil {
			t.Fatal("ambiguous/stale outcome changed state", got)
		}
	}
	s := &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 3, MaxAttempts: 3}, completeErr: inbound.ErrUnavailable}
	if result, err := processor(t, s).ProcessOne(t.Context()); err != nil || result.Outcome != "failed" {
		t.Fatal(result, err)
	}
	empty := &fakeStore{}
	if r, e := processor(t, empty).ProcessOne(t.Context()); e != nil || r.Found {
		t.Fatal(r, e)
	}
	empty.claimErr = inbound.ErrUnavailable
	if _, e := processor(t, empty).ProcessOne(t.Context()); !errors.Is(e, inbound.ErrUnavailable) {
		t.Fatal(e)
	}
}
func TestBackoffAndOutputFailure(t *testing.T) {
	s := &fakeStore{item: &outbound.Item{Event: event(t), Attempt: 1, MaxAttempts: 2}}
	p := processor(t, s)
	for _, attempt := range []int{1, 2, 1000} {
		d := p.backoff(attempt)
		if d < time.Millisecond/2 || d > time.Second {
			t.Fatal(d)
		}
	}
	small, _ := jsondocument.New(1)
	p.encoder = small
	r, err := p.ProcessOne(t.Context())
	if err != nil || r.Outcome != "failed" || s.failure.Code != "output_too_large" {
		t.Fatal(r, err)
	}
	if _, err := NewAccept(domain.ChainID("01"), 1, s); err == nil {
		t.Fatal("invalid config")
	}
	if _, err := NewProcess(nil, ethereumjson.Decoder{}, small, 1, 2); err == nil {
		t.Fatal("invalid config")
	}
}
