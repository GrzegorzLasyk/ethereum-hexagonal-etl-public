// process.go implements Inbox processing, validation and bounded failure orchestration in the
// application orchestration layer.

package usecases

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
)

// Process coordinates a claimed Inbox item, codecs and atomic completion with bounded retry
// classification.
type Process struct {
	queue        outbound.Queue
	decoder      outbound.Decoder
	initial, max time.Duration
}

// NewProcess validates the queue/codec ports and backoff bounds before constructing an
// asynchronous processing use case.
func NewProcess(q outbound.Queue, d outbound.Decoder, initial, max time.Duration) (*Process, error) {
	if q == nil || d == nil || initial <= 0 || max < initial {
		return nil, fmt.Errorf("invalid processing configuration")
	}
	return &Process{q, d, initial, max}, nil
}

// ProcessOne claims and processes one Inbox item, returning its identity/outcome; ambiguous
// completion or a lost lease must not be followed by a failure mutation.
func (u *Process) ProcessOne(ctx context.Context) (inbound.ProcessResult, error) {
	item, err := u.queue.Claim(ctx)
	if err != nil {
		return inbound.ProcessResult{}, fmt.Errorf("claim inbox: %w", err)
	}
	if item == nil {
		return inbound.ProcessResult{}, nil
	}
	progress := inbound.ProcessResult{Found: true, EventID: item.Event.EventID, SourceEventID: item.Event.SourceEventID, Attempt: item.Attempt}
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
	progress.ErrorCode = failure.Code
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

// decode decodes the normalized document, validates envelope/provenance binding and checks
// cancellation before persistence.
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

// classify separates permanent invalid/incomplete/identity failures from transient processing
// errors using sentinel identities.
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

// backoff calculates bounded exponential delay with jitter from the durable attempt number.
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
