package inbound

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidEvent       = errors.New("invalid event")
	ErrUnsupportedVersion = errors.New("unsupported event version")
	ErrConflict           = errors.New("event identity conflict")
	ErrUnavailable        = errors.New("inbox unavailable")
	ErrNotImplemented     = errors.New("durable inbox acceptance is not implemented")
)

// BlockIdentity is service-owned application input, not a transport DTO.
type BlockIdentity struct {
	ChainID    string
	Number     uint64
	Hash       [32]byte
	ParentHash [32]byte
}

type AcceptTransformedBlockRequest struct {
	EventID        string
	EventVersion   uint32
	PayloadVersion uint32
	Block          BlockIdentity
	Provider       string
	ExtractedAt    time.Time
	PayloadJSON    []byte
	SourceEventID  string
	TransformedAt  time.Time
}

type Acceptance uint8

const (
	AcceptanceUnspecified Acceptance = iota
	DurablyAccepted
	AlreadyAccepted
)

type AcceptanceResult struct {
	EventID    string
	Acceptance Acceptance
}

// TransformedBlockAcceptor must return a positive result ONLY after a local durable
// Inbox commit, or after verifying a previously committed identical event.
// Conflicting content under the same event ID returns ErrConflict. A zero result
// is not success. An ambiguous commit remains an error; retry the stable ID.
// Acceptance says nothing about downstream processing or completion.
//
// Request buffers are borrowed, immutable for the duration of the call. Do not
// mutate or retain them after return; persist before ACK. No goroutine may keep
// an acknowledged event only in memory. The context applies to acceptance I/O.
type TransformedBlockAcceptor interface {
	AcceptTransformedBlock(context.Context, AcceptTransformedBlockRequest) (AcceptanceResult, error)
}
