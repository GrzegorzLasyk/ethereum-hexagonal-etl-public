package usecases

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type IndexOptions struct {
	Chain                        domain.ChainID
	Start                        uint64
	End                          *uint64
	Confirmations, RecoveryDepth uint64
	Batch                        int
	Limits                       domain.CapacityLimits
}

func (o IndexOptions) Validate() error {
	if err := o.Chain.Validate(); err != nil {
		return err
	}
	if o.End != nil && *o.End < o.Start || o.Batch < 1 || o.Batch > 1000 || o.RecoveryDepth < 1 || o.RecoveryDepth > 10000 {
		return fmt.Errorf("%w: invalid range, batch or recovery depth", domain.ErrInvalidData)
	}
	return o.Limits.Validate()
}

type Index struct {
	options  IndexOptions
	source   outbound.BlockSource
	observer outbound.ChainObserver
	store    outbound.BranchStore
}

var _ inbound.Indexer = (*Index)(nil)

func NewIndex(o IndexOptions, source outbound.BlockSource, observer outbound.ChainObserver, store outbound.BranchStore) (*Index, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if source == nil || observer == nil || store == nil {
		return nil, domain.ErrInvalidData
	}
	if o.End != nil {
		end := *o.End
		o.End = &end
	}
	return &Index{o, source, observer, store}, nil
}
func validHeader(h domain.Header, chain domain.ChainID, n uint64) error {
	if err := h.ID.Validate(); err != nil {
		return err
	}
	if h.ID.ChainID != chain {
		return domain.ErrChainMismatch
	}
	if h.ID.Number != n || n != 0 && h.ParentHash == (domain.Hash{}) {
		return domain.ErrInvalidData
	}
	return nil
}
func (u *Index) Cycle(ctx context.Context) (inbound.IndexResult, error) {
	var result inbound.IndexResult
	for result.Processed < u.options.Batch {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		cp, err := u.store.Checkpoint(ctx, u.options.Chain)
		if err != nil {
			return result, err
		}
		if cp != nil {
			copy := *cp
			cp = &copy
			if cp.Block.Number < u.options.Start {
				return result, domain.ErrCheckpointConflict
			}
		}
		head, err := u.observer.Head(ctx)
		if err != nil {
			return result, fmt.Errorf("observe head: %w", err)
		}
		if err := validHeader(head, u.options.Chain, head.ID.Number); err != nil {
			return result, err
		}
		if cp != nil {
			if head.ID.Number < cp.Block.Number {
				return result, fmt.Errorf("%w: provider head below checkpoint", domain.ErrRecoveryRequired)
			}
			tip, err := u.observer.Header(ctx, cp.Block.Number)
			if err != nil {
				return result, fmt.Errorf("verify checkpoint: %w", err)
			}
			if err := validHeader(tip, u.options.Chain, cp.Block.Number); err != nil {
				return result, err
			}
			if head.ID.Number == tip.ID.Number && (head.ID != tip.ID || head.ParentHash != tip.ParentHash) {
				return result, domain.ErrUnstableChain
			}
			local, err := u.store.ActiveHeader(ctx, cp.Block.Number)
			if err != nil {
				return result, fmt.Errorf("%w: checkpoint missing from active branch: %w", domain.ErrRecoveryRequired, err)
			}
			if local.ID != cp.Block {
				return result, domain.ErrCheckpointConflict
			}
			if tip.ID == local.ID && tip.ParentHash != local.ParentHash {
				return result, domain.ErrUnstableChain
			}
			if tip.ID != cp.Block {
				if err := u.reconcile(ctx, *cp, tip, head); err != nil {
					return result, err
				}
				result.Rewound = true
				return result, nil
			}
		}
		if cp != nil && (u.options.End != nil && cp.Block.Number >= *u.options.End || cp.Block.Number == math.MaxUint64) {
			result.Done = true
			return result, nil
		}
		next := u.options.Start
		if cp != nil {
			next = cp.Block.Number + 1
		}
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
			return result, fmt.Errorf("fetch block %d: %w", next, err)
		}
		if err := e.Validate(); err != nil {
			return result, err
		}
		if err := validHeader(e.Header, u.options.Chain, next); err != nil {
			return result, err
		}
		if err := domain.CheckContinuity(e.Header, cp); err != nil {
			return result, fmt.Errorf("%w: %w", domain.ErrUnstableChain, err)
		}
		// Recheck both the fetched identity and the sampled confirmation anchor.
		if err := verifyObservation(ctx, u.observer, e.Header, head); err != nil {
			return result, err
		}
		_, err = u.store.Commit(ctx, outbound.CommitExtraction{Extraction: e, ExpectedCheckpoint: cp, Limits: &u.options.Limits})
		if errors.Is(err, domain.ErrBackpressure) {
			result.Backpressure = true
			return result, nil
		}
		if err != nil {
			return result, err
		}
		result.Processed++
	}
	return result, nil
}
func verifyObservation(ctx context.Context, o outbound.ChainObserver, block, anchor domain.Header) error {
	for _, h := range []domain.Header{block, anchor} {
		now, err := o.Header(ctx, h.ID.Number)
		if err != nil {
			return err
		}
		if now.ID != h.ID || now.ParentHash != h.ParentHash {
			return domain.ErrUnstableChain
		}
	}
	return nil
}
func (u *Index) reconcile(ctx context.Context, cp domain.Checkpoint, tip, head domain.Header) error {
	current := tip
	var oldChild *domain.Header
	for depth := uint64(0); ; depth++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		local, err := u.store.ActiveHeader(ctx, current.ID.Number)
		if err != nil {
			return fmt.Errorf("%w: retained ancestor unavailable: %w", domain.ErrRecoveryRequired, err)
		}
		if err := validHeader(local, u.options.Chain, current.ID.Number); err != nil {
			return err
		}
		if depth == 0 && local.ID != cp.Block {
			return domain.ErrCheckpointConflict
		}
		if oldChild != nil && oldChild.ParentHash != local.ID.Hash {
			return fmt.Errorf("%w: retained lineage is discontinuous", domain.ErrRecoveryRequired)
		}
		if current.ID == local.ID {
			if current.ParentHash != local.ParentHash {
				return domain.ErrUnstableChain
			}
			if err := verifyObservation(ctx, u.observer, current, tip); err != nil {
				return err
			}
			if err := verifyObservation(ctx, u.observer, tip, head); err != nil {
				return err
			}
			return u.store.Rewind(ctx, cp, current.ID)
		}
		if depth == u.options.RecoveryDepth || current.ID.Number == u.options.Start {
			return fmt.Errorf("%w: no common ancestor within retained range and recovery depth", domain.ErrRecoveryRequired)
		}
		if current.ID.Number < u.options.Start {
			return domain.ErrRecoveryRequired
		}
		parent, err := u.observer.Header(ctx, current.ID.Number-1)
		if err != nil {
			return fmt.Errorf("read candidate ancestor: %w", err)
		}
		if err := validHeader(parent, u.options.Chain, current.ID.Number-1); err != nil {
			return err
		}
		if current.ParentHash != parent.ID.Hash {
			return domain.ErrUnstableChain
		}
		oldChild = &local
		current = parent
	}
}
