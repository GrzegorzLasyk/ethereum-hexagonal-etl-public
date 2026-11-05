// validation.go defines contracts for pure value, membership and identity validation at the local
// boundary in the application inbound boundary.

package inbound

import (
	"fmt"
	"regexp"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/domain"
)

// providerPattern restricts source-provider metadata to a bounded non-secret label vocabulary.
var providerPattern = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

// ValidateEnvelope checks application invariants independently of gRPC. Semantic
// payload errors are accepted durably, then recorded by the asynchronous worker.
func ValidateEnvelope(r AcceptRawBlockRequest, chain domain.ChainID, maxBytes int) error {
	if r.EventVersion != 1 || r.PayloadVersion != 1 {
		return ErrUnsupportedVersion
	}
	b := r.Block
	if !domain.ChainID(b.ChainID).Valid() || b.ChainID != string(chain) || b.Hash == ([32]byte{}) || b.Number != 0 && b.ParentHash == ([32]byte{}) ||
		r.EventID != fmt.Sprintf("raw-block/v1/%s/%d/%x", b.ChainID, b.Number, b.Hash) || !providerPattern.MatchString(r.Provider) ||
		r.ExtractedAt.IsZero() || r.ExtractedAt.Year() < 1 || r.ExtractedAt.Year() > 9999 || len(r.RawBlock) == 0 {
		return ErrInvalidEvent
	}
	remaining := maxBytes - len(r.RawBlock)
	if remaining < 0 {
		return ErrInvalidEvent
	}
	for _, receipt := range r.RawReceipts {
		if len(receipt) == 0 || len(receipt) > remaining {
			return ErrInvalidEvent
		}
		remaining -= len(receipt)
	}
	return nil
}
