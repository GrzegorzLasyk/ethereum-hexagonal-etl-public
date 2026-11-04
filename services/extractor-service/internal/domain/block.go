// block.go implements extraction identity, complete raw membership and source ownership checks in
// the pure domain layer.

// Package domain provides extraction identity, complete raw membership and source ownership checks
// in the pure domain layer.
package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

var (
	// ErrInvalidData signals invalid extraction data. Callers can match this category with errors.Is
	// without parsing error text.
	ErrInvalidData = errors.New("invalid extraction data")
	// ErrChainMismatch signals chain mismatch. Callers can match this category with errors.Is without
	// parsing error text.
	ErrChainMismatch = errors.New("chain mismatch")
	// ErrUnavailable signals source data unavailable. Callers can match this category with errors.Is
	// without parsing error text.
	ErrUnavailable = errors.New("source data unavailable")
	// ErrIncomplete signals incomplete block. Callers can match this category with errors.Is without
	// parsing error text.
	ErrIncomplete = errors.New("incomplete block")
	// ErrBlockConflict signals block identity or parent conflict. Callers can match this category with
	// errors.Is without parsing error text.
	ErrBlockConflict = errors.New("block identity or parent conflict")
	// ErrCheckpointConflict signals checkpoint conflict. Callers can match this category with
	// errors.Is without parsing error text.
	ErrCheckpointConflict = errors.New("checkpoint conflict")
	// ErrOutOfSequence signals block is outside the extraction sequence. Callers can match this
	// category with errors.Is without parsing error text.
	ErrOutOfSequence = errors.New("block is outside the extraction sequence")
)

// ChainID is a canonical nonnegative decimal integer, not an RPC hex quantity.
type ChainID string

// Validate requires a canonical nonnegative decimal chain identifier without imposing a uint64
// chain limit.
func (id ChainID) Validate() error {
	if len(id) == 0 || (len(id) > 1 && id[0] == '0') {
		return fmt.Errorf("%w: noncanonical chain ID", ErrInvalidData)
	}
	for _, digit := range id {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("%w: chain ID must contain decimal digits", ErrInvalidData)
		}
	}
	return nil
}

// Hash is a fixed 32-byte Ethereum identity; a height alone is not sufficient to distinguish
// forks.
type Hash [32]byte

// Address is a fixed 20-byte Ethereum address; optional recipients represent null separately.
type Address [20]byte

// BlockID combines chain, height and hash so historical competing observations keep distinct
// identities.
type BlockID struct {
	ChainID ChainID
	Number  uint64
	Hash    Hash
}

// Validate checks chain and nonzero block hash so height alone cannot represent an observation.
func (id BlockID) Validate() error {
	if err := id.ChainID.Validate(); err != nil {
		return err
	}
	if id.Hash == (Hash{}) {
		return fmt.Errorf("%w: missing block hash", ErrInvalidData)
	}
	return nil
}

// EventID identifies one raw-block event independently of retries and provenance.
func (id BlockID) EventID() string {
	return fmt.Sprintf("raw-block/v1/%s/%d/%x", id.ChainID, id.Number, id.Hash)
}

// Header contains the mined block identity and parent link needed for continuity/recovery checks.
type Header struct {
	ID         BlockID
	ParentHash Hash
	Timestamp  uint64
}

// Transaction describes membership; the complete body remains in RawBlock.
type Transaction struct {
	Block BlockID
	Hash  Hash
	Index uint64
}

// Receipt describes completeness and membership, not the normalized outcome.
type Receipt struct {
	Block            BlockID
	TransactionHash  Hash
	TransactionIndex uint64
	Logs             []Log
}

// Log models a receipt log with block/transaction position, ordered topics and data.
type Log struct {
	Block            BlockID
	TransactionHash  Hash
	TransactionIndex uint64
	Index            uint64
	Address          Address
	Topics           []Hash
	Data             []byte
	Removed          bool
}

// Extraction is a complete source snapshot. Bytes are opaque to this domain;
// the source adapter must validate their format and correspondence to metadata.
type Extraction struct {
	Header       Header
	Transactions []Transaction
	Receipts     []Receipt
	RawBlock     []byte
	RawReceipts  [][]byte
	Provider     string
	ExtractedAt  time.Time
}

// Validate checks complete raw/typed membership, deterministic indexes and provenance before
// persistence can accept a snapshot.
func (e Extraction) Validate() error {
	if err := e.Header.ID.Validate(); err != nil {
		return err
	}
	if e.Header.ID.Number != 0 && e.Header.ParentHash == (Hash{}) {
		return fmt.Errorf("%w: missing non-genesis parent hash", ErrInvalidData)
	}
	if strings.TrimSpace(e.Provider) == "" || e.ExtractedAt.IsZero() {
		return fmt.Errorf("%w: missing source provenance", ErrInvalidData)
	}
	if len(e.RawBlock) == 0 || e.Transactions == nil || e.Receipts == nil || e.RawReceipts == nil {
		return fmt.Errorf("%w: missing block or collections", ErrIncomplete)
	}
	if len(e.Transactions) != len(e.Receipts) || len(e.Transactions) != len(e.RawReceipts) {
		return fmt.Errorf("%w: expected one receipt per transaction", ErrIncomplete)
	}
	seen := make(map[Hash]struct{}, len(e.Transactions))
	var nextLog uint64
	for i, tx := range e.Transactions {
		if tx.Hash == (Hash{}) || tx.Index != uint64(i) {
			return fmt.Errorf("%w: transaction %d identity/index", ErrInvalidData, i)
		}
		if _, exists := seen[tx.Hash]; exists {
			return fmt.Errorf("%w: duplicate transaction %d", ErrInvalidData, i)
		}
		seen[tx.Hash] = struct{}{}
		if err := sameBlock(e.Header.ID, tx.Block); err != nil {
			return fmt.Errorf("transaction %d: %w", i, err)
		}
		receipt := e.Receipts[i]
		if err := sameBlock(e.Header.ID, receipt.Block); err != nil {
			return fmt.Errorf("receipt %d: %w", i, err)
		}
		if receipt.TransactionHash != tx.Hash || receipt.TransactionIndex != tx.Index {
			return fmt.Errorf("%w: receipt %d transaction membership", ErrInvalidData, i)
		}
		if len(e.RawReceipts[i]) == 0 || receipt.Logs == nil {
			return fmt.Errorf("%w: receipt %d body/logs", ErrIncomplete, i)
		}
		for _, log := range receipt.Logs {
			if err := sameBlock(e.Header.ID, log.Block); err != nil {
				return fmt.Errorf("log %d: %w", log.Index, err)
			}
			if log.Removed {
				return fmt.Errorf("%w: removed log", ErrBlockConflict)
			}
			if log.TransactionHash != tx.Hash || log.TransactionIndex != tx.Index ||
				log.Index != nextLog || len(log.Topics) > 4 {
				return fmt.Errorf("%w: log membership/index/topics", ErrInvalidData)
			}
			nextLog++
		}
	}
	return nil
}

// sameBlock distinguishes chain mismatch from another full block identity before accepting
// transaction/receipt membership.
func sameBlock(expected, actual BlockID) error {
	if expected.ChainID != actual.ChainID {
		return ErrChainMismatch
	}
	if expected != actual {
		return ErrBlockConflict
	}
	return nil
}

// Clone gives the recipient ownership of mutable source buffers.
func (e Extraction) Clone() Extraction {
	e.Transactions = slices.Clone(e.Transactions)
	e.RawBlock = slices.Clone(e.RawBlock)
	e.RawReceipts = slices.Clone(e.RawReceipts)
	for i := range e.RawReceipts {
		e.RawReceipts[i] = slices.Clone(e.RawReceipts[i])
	}
	e.Receipts = slices.Clone(e.Receipts)
	for i := range e.Receipts {
		e.Receipts[i].Logs = slices.Clone(e.Receipts[i].Logs)
		for j := range e.Receipts[i].Logs {
			log := &e.Receipts[i].Logs[j]
			log.Topics = slices.Clone(log.Topics)
			log.Data = slices.Clone(log.Data)
		}
	}
	return e
}
