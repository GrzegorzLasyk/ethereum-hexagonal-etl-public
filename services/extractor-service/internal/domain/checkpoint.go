package domain

import (
	"fmt"
	"math"
)

// Checkpoint is the last block committed with its source data and outbox intent.
type Checkpoint struct {
	Block    BlockID
	Revision uint64
	Start    uint64
}

// CheckPosition permits initial extraction, immediate continuation, and retry
// of the checkpoint height. Historical replay is intentionally not supported.
func CheckPosition(chain ChainID, start, requested uint64, previous *Checkpoint) error {
	if err := chain.Validate(); err != nil {
		return err
	}
	if previous == nil {
		if requested != start {
			return fmt.Errorf("%w: first block must equal configured start", ErrOutOfSequence)
		}
		return nil
	}
	if err := previous.Block.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrCheckpointConflict, err)
	}
	if previous.Block.ChainID != chain {
		return ErrChainMismatch
	}
	if previous.Block.Number < start {
		return fmt.Errorf("%w: checkpoint precedes configured start", ErrCheckpointConflict)
	}
	if requested == previous.Block.Number {
		return nil
	}
	if previous.Block.Number != math.MaxUint64 && requested == previous.Block.Number+1 {
		return nil
	}
	return ErrOutOfSequence
}

func CheckContinuity(header Header, previous *Checkpoint) error {
	if previous == nil {
		return nil
	}
	if header.ID.ChainID != previous.Block.ChainID {
		return ErrChainMismatch
	}
	if header.ID.Number == previous.Block.Number {
		if header.ID.Hash != previous.Block.Hash {
			return ErrBlockConflict
		}
		return nil
	}
	if previous.Block.Number == math.MaxUint64 || header.ID.Number != previous.Block.Number+1 {
		return ErrOutOfSequence
	}
	if header.ParentHash != previous.Block.Hash {
		return ErrBlockConflict
	}
	return nil
}
