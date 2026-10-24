package outbound

import (
	"context"
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ErrRetryDeferred prevents a scheduling loop from bypassing a provider cooldown.
var ErrRetryDeferred = errors.New("provider cooldown requires a later operator restart")

// ChainObserver reads the configured provider's current branch. Observations
// are not finality proofs or a consistent snapshot across separate calls.
// Missing blocks are errors, never a signal to rewind durable progress.
type ChainObserver interface {
	Head(context.Context) (domain.Header, error)
	Header(context.Context, uint64) (domain.Header, error)
}
