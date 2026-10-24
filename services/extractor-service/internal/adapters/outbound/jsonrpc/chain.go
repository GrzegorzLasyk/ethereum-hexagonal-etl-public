package jsonrpc

import (
	"context"
	"fmt"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

var _ outbound.ChainObserver = (*Client)(nil)

// Head observes latest, including its identity, rather than treating a height
// alone as an immutable chain tip. It never requests pending or implies finality.
func (c *Client) Head(ctx context.Context) (domain.Header, error) {
	return c.header(ctx, "latest", nil)
}

func (c *Client) Header(ctx context.Context, number uint64) (domain.Header, error) {
	return c.header(ctx, fmt.Sprintf("0x%x", number), &number)
}

func (c *Client) header(ctx context.Context, tag string, expected *uint64) (domain.Header, error) {
	var empty domain.Header
	chain, err := c.ChainID(ctx)
	if err != nil {
		return empty, fmt.Errorf("verify header source chain: %w", err)
	}
	raw, err := c.call(ctx, "eth_getBlockByNumber", []any{tag, false})
	if err != nil {
		return empty, fmt.Errorf("fetch header: %w", err)
	}
	o, err := uniqueObject(raw)
	if err != nil {
		return empty, err
	}
	id, err := blockReference(o, chain, "number", "hash")
	if err != nil {
		return empty, err
	}
	if expected != nil && id.Number != *expected {
		return empty, invalid("requested block number")
	}
	parent, err := hashField(o, "parentHash")
	if err != nil {
		return empty, err
	}
	if id.Number != 0 && parent == (domain.Hash{}) {
		return empty, invalid("non-genesis parentHash")
	}
	timestamp, err := uintField(o, "timestamp")
	if err != nil {
		return empty, err
	}
	if _, err := c.ChainID(ctx); err != nil {
		return empty, fmt.Errorf("recheck header source chain: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return domain.Header{ID: id, ParentHash: parent, Timestamp: timestamp}, nil
}
