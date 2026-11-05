// validation.go defines contracts for pure value, membership and identity validation at the local
// boundary in the application inbound boundary.

package inbound

import (
	"fmt"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
	"regexp"
)

// providerPattern restricts source-provider metadata to a bounded non-secret label vocabulary.
var providerPattern = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

// ValidateEnvelope checks application invariants independently of gRPC. Semantic
// payload errors are accepted durably, then recorded by the asynchronous worker.
func ValidateEnvelope(r AcceptTransformedBlockRequest, chain domain.ChainID, maxBytes int) error {
	if r.EventVersion != 1 || r.PayloadVersion != 1 {
		return ErrUnsupportedVersion
	}
	b := r.Block
	if !domain.ChainID(b.ChainID).Valid() || b.ChainID != string(chain) || b.Hash == ([32]byte{}) || b.Number != 0 && b.ParentHash == ([32]byte{}) ||
		r.EventID != fmt.Sprintf("transformed-block/v1/%s/%d/%x", b.ChainID, b.Number, b.Hash) || !providerPattern.MatchString(r.Provider) ||
		r.ExtractedAt.IsZero() || r.ExtractedAt.Year() < 1 || r.ExtractedAt.Year() > 9999 || len(r.PayloadJSON) == 0 || len(r.PayloadJSON) > maxBytes ||
		r.SourceEventID != fmt.Sprintf("raw-block/v1/%s/%d/%x", b.ChainID, b.Number, b.Hash) ||
		r.TransformedAt.IsZero() || r.TransformedAt.Year() < 1 || r.TransformedAt.Year() > 9999 {
		return ErrInvalidEvent
	}
	return nil
}

// ValidateDocument binds normalized content to the accepted immutable envelope.
func ValidateDocument(r AcceptTransformedBlockRequest, doc domain.Document) error {
	if err := doc.Snapshot.Validate(); err != nil {
		return err
	}
	b := doc.Snapshot.Block
	if string(doc.Snapshot.ChainID) != r.Block.ChainID || b.Number != r.Block.Number ||
		[32]byte(b.Hash) != r.Block.Hash || [32]byte(b.ParentHash) != r.Block.ParentHash ||
		doc.Provenance.SourceEventID != r.SourceEventID || doc.Provenance.Provider != r.Provider ||
		!doc.Provenance.ExtractedAt.Equal(r.ExtractedAt) {
		return domain.ErrIdentity
	}
	return nil
}
