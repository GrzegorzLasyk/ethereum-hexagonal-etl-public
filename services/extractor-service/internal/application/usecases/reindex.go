// reindex.go implements independent historical range cursors and original-event replay in the
// application orchestration layer.

package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// Reindex orchestrates an independent bounded historical cursor while preserving the active
// extraction checkpoint.
type Reindex struct {
	options  IndexOptions
	request  outbound.ReindexRequest
	source   outbound.BlockSource
	observer outbound.ChainObserver
	store    outbound.ReindexStore
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ inbound.Indexer = (*Reindex)(nil)

// NewReindex validates the independent historical range and injected ports without changing the
// live checkpoint.
func NewReindex(o IndexOptions, id string, source outbound.BlockSource, observer outbound.ChainObserver, store outbound.ReindexStore) (*Reindex, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o.End == nil || domain.ValidateOperationID(id) != nil || source == nil || observer == nil || store == nil {
		return nil, domain.ErrInvalidData
	}
	return &Reindex{o, outbound.ReindexRequest{ID: id, Start: o.Start, End: *o.End}, source, observer, store}, nil
}

// Cycle resumes the separate historical range cursor and commits complete observations without
// advancing the active checkpoint.
func (u *Reindex) Cycle(ctx context.Context) (result inbound.IndexResult, err error) {
	result.ChainID = string(u.options.Chain)
	result.Reindex = true
	started := time.Now()
	defer func() { result.Duration = time.Since(started) }()
	for result.Processed < u.options.Batch {
		started = time.Now()
		result.Block, result.Next = nil, nil
		state, err := u.store.OpenReindex(ctx, u.request)
		if err != nil {
			return result, err
		}
		result.Block = state.Last
		head, err := u.observer.Head(ctx)
		if err != nil {
			return result, err
		}
		if err := validHeader(head, u.options.Chain, head.ID.Number); err != nil {
			return result, err
		}
		height := head.ID.Number
		result.Head = &height
		result.HeadObservedAt = time.Now()
		if state.Last != nil {
			last, err := u.observer.Header(ctx, state.Last.Number)
			if err != nil {
				return result, err
			}
			if last.ID != *state.Last || head.ID.Number < state.Last.Number {
				return result, fmt.Errorf("%w: reindex branch changed; investigate and use a new operation ID", domain.ErrRecoveryRequired)
			}
			if state.Last.Number == u.request.End {
				result.Done = true
				return result, nil
			}
		}
		next := u.request.Start
		if state.Last != nil {
			next = state.Last.Number + 1
		}
		result.Block = nil
		result.Next = &next
		eligible, ok := domain.EligibleHeight(head.ID.Number, u.options.Confirmations)
		if !ok || next > eligible {
			result.Waiting = true
			return result, nil
		}
		capacity, err := u.store.Capacity(ctx)
		if err != nil {
			return result, err
		}
		if !capacity.Allows(u.options.Limits, 0) {
			result.Backpressure = true
			return result, nil
		}
		e, err := u.source.Block(ctx, next)
		if err != nil {
			return result, err
		}
		result.Block = &e.Header.ID
		if err := e.Validate(); err != nil {
			return result, err
		}
		if err := validHeader(e.Header, u.options.Chain, next); err != nil {
			return result, err
		}
		if state.Last != nil && e.Header.ParentHash != state.Last.Hash {
			return result, domain.ErrUnstableChain
		}
		if err := verifyObservation(ctx, u.observer, e.Header, head); err != nil {
			return result, err
		}
		if err := u.store.CommitReindex(ctx, state, e, u.options.Limits); err != nil {
			if errors.Is(err, domain.ErrBackpressure) {
				result.Backpressure = true
				return result, nil
			}
			return result, err
		}
		result.Processed++
		result.Completed = append(result.Completed, inbound.IndexedBlock{Block: e.Header.ID, Duration: time.Since(started)})
	}
	return result, nil
}

// Replay grants one retained event a bounded fresh delivery allowance. It does
// not reset a downstream inbox or change immutable source/transformation data.
func Replay(ctx context.Context, store outbound.ReplayStore, r outbound.ReplayRequest) error {
	if store == nil || domain.ValidateOperationID(r.ID) != nil || r.EventID == "" || r.Allowance < 1 || r.Allowance > 1000 {
		return domain.ErrInvalidData
	}
	return store.Replay(ctx, r)
}
