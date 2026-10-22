package usecases

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

type Process struct {
	queue        outbound.Queue
	decoder      outbound.Decoder
	initial, max time.Duration
}

func NewProcess(q outbound.Queue, d outbound.Decoder, initial, max time.Duration) (*Process, error) {
	if q == nil || d == nil || initial <= 0 || max < initial {
		return nil, fmt.Errorf("invalid processing configuration")
	}
	return &Process{q, d, initial, max}, nil
}
func (u *Process) ProcessOne(ctx context.Context) (inbound.ProcessResult, error) {
	item, err := u.queue.Claim(ctx)
	if err != nil {
		return inbound.ProcessResult{}, fmt.Errorf("claim inbox: %w", err)
	}
	if item == nil {
		return inbound.ProcessResult{}, nil
	}
	progress := inbound.ProcessResult{Found: true, EventID: item.Event.EventID, Attempt: item.Attempt}
	result, err := u.decode(ctx, item.Event)
	if err == nil {
		err = u.queue.Complete(ctx, *item, result)
		if err == nil {
			progress.Outcome = "completed"
			return progress, nil
		}
		// Never follow an ambiguous commit with another state change. A later claim
		// sees either completion or an expired lease; the records/completion stay atomic.
		if errors.Is(err, outbound.ErrCommitUnknown) || errors.Is(err, outbound.ErrLeaseLost) {
			return progress, err
		}
	}
	if ctx.Err() != nil {
		return progress, ctx.Err()
	} // lease recovery after shutdown
	failure := classify(err)
	failure.Delay = u.backoff(item.Attempt)
	if saveErr := u.queue.Fail(ctx, *item, failure); saveErr != nil {
		return progress, fmt.Errorf("record failure: %w", saveErr)
	}
	progress.Outcome = "retry"
	if failure.Permanent || item.Attempt >= item.MaxAttempts {
		progress.Outcome = "failed"
	}
	return progress, nil
}
func (u *Process) decode(ctx context.Context, r inbound.AcceptTransformedBlockRequest) (domain.Document, error) {
	doc, err := u.decoder.Decode(ctx, r.PayloadJSON)
	if err != nil {
		return domain.Document{}, err
	}
	if err := inbound.ValidateDocument(r, doc); err != nil {
		return domain.Document{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.Document{}, err
	}
	return doc, nil
}

func classify(err error) outbound.Failure {
	for _, entry := range []struct {
		err  error
		code string
	}{
		{domain.ErrIncomplete, "incomplete"}, {domain.ErrIdentity, "identity"}, {domain.ErrUnsupported, "unsupported"}, {domain.ErrInvalid, "invalid"}, {inbound.ErrConflict, "conflict"},
	} {
		if errors.Is(err, entry.err) {
			return outbound.Failure{Code: entry.code, Permanent: true}
		}
	}
	return outbound.Failure{Code: "transient"}
}
func (u *Process) backoff(attempt int) time.Duration {
	delay := u.initial
	for i := 1; i < attempt && delay < u.max; i++ {
		if delay > u.max/2 {
			delay = u.max
			break
		}
		delay *= 2
	}
	if delay > u.max {
		delay = u.max
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}
