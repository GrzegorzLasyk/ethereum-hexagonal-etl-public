package usecases

import (
	"context"
	"fmt"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

type Accept struct {
	chain    domain.ChainID
	maxBytes int
	inbox    outbound.Inbox
}

func NewAccept(chain domain.ChainID, maxBytes int, inbox outbound.Inbox) (*Accept, error) {
	if !chain.Valid() || maxBytes <= 0 || maxBytes > 64<<20 || inbox == nil {
		return nil, fmt.Errorf("invalid acceptance configuration")
	}
	return &Accept{chain, maxBytes, inbox}, nil
}
func (u *Accept) AcceptTransformedBlock(ctx context.Context, r inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
	if err := ctx.Err(); err != nil {
		return inbound.AcceptanceResult{}, err
	}
	if err := inbound.ValidateEnvelope(r, u.chain, u.maxBytes); err != nil {
		return inbound.AcceptanceResult{}, err
	}
	duplicate, err := u.inbox.Accept(ctx, r)
	if err != nil {
		return inbound.AcceptanceResult{}, fmt.Errorf("accept raw block: %w", err)
	}
	outcome := inbound.DurablyAccepted
	if duplicate {
		outcome = inbound.AlreadyAccepted
	}
	return inbound.AcceptanceResult{EventID: r.EventID, Acceptance: outcome}, nil
}
