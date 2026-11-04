// accept_block.go defines contracts for plain event envelopes and explicit durable acceptance
// contracts in the application inbound boundary.

// Package inbound defines plain application entry contracts and progress/acceptance outcomes,
// independent of transport DTOs.
package inbound

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidEvent signals invalid event. Callers can match this category with errors.Is without
	// parsing error text.
	ErrInvalidEvent = errors.New("invalid event")
	// ErrUnsupportedVersion signals unsupported event version. Callers can match this category with
	// errors.Is without parsing error text.
	ErrUnsupportedVersion = errors.New("unsupported event version")
	// ErrConflict signals event identity conflict. Callers can match this category with errors.Is
	// without parsing error text.
	ErrConflict = errors.New("event identity conflict")
	// ErrOverloaded signals durable admission capacity exhausted. Callers can match this category with
	// errors.Is without parsing error text.
	ErrOverloaded = errors.New("durable admission capacity exhausted")
	// ErrUnavailable signals inbox unavailable. Callers can match this category with errors.Is without
	// parsing error text.
	ErrUnavailable = errors.New("inbox unavailable")
	// ErrNotImplemented signals durable inbox acceptance is not implemented. Callers can match this
	// category with errors.Is without parsing error text.
	ErrNotImplemented = errors.New("durable inbox acceptance is not implemented")
)

// BlockIdentity is service-owned application input, not a transport DTO.
type BlockIdentity struct {
	ChainID    string
	Number     uint64
	Hash       [32]byte
	ParentHash [32]byte
}

// AcceptRawBlockRequest carries plain metadata and borrowed exact raw payloads into the durable
// acceptance boundary.
type AcceptRawBlockRequest struct {
	EventID        string
	EventVersion   uint32
	PayloadVersion uint32
	Block          BlockIdentity
	Provider       string
	ExtractedAt    time.Time
	RawBlock       []byte
	RawReceipts    [][]byte
}

// Acceptance encodes whether acceptance is unspecified, newly durable or an identical already-
// durable event.
type Acceptance uint8

const (
	// AcceptanceUnspecified is the zero result and must never be interpreted as a durable ACK.
	AcceptanceUnspecified Acceptance = iota
	// DurablyAccepted means new input committed to the receiver Inbox; asynchronous processing may
	// still be pending.
	DurablyAccepted
	// AlreadyAccepted means the same event/content was already durable; no new processing item is
	// created.
	AlreadyAccepted
)

// AcceptanceResult returns durable acceptance kind and event ID; callers must not infer success
// from a zero value.
type AcceptanceResult struct {
	EventID    string
	Acceptance Acceptance
}

// RawBlockAcceptor must return a positive result ONLY after a local durable
// Inbox commit, or after verifying a previously committed identical event.
// Conflicting content under the same event ID returns ErrConflict. A zero result
// is not success. An ambiguous commit remains an error; retry the stable ID.
// Acceptance says nothing about downstream processing or completion.
//
// Request buffers are borrowed, immutable for the duration of the call. Do not
// mutate or retain them after return; persist before ACK. No goroutine may keep
// an acknowledged event only in memory. The context applies to acceptance I/O.
type RawBlockAcceptor interface {
	// AcceptRawBlock validates and durably accepts exact raw input before its result may be
	// acknowledged.
	AcceptRawBlock(context.Context, AcceptRawBlockRequest) (AcceptanceResult, error)
}
