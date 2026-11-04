// receipts.go implements bounded concurrent receipt fetching with indexed results and joined
// cancellation in the outbound adapter layer.

package jsonrpc

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// DefaultReceiptConcurrency keeps receipt acquisition sequential unless the operator explicitly
// selects more workers.
const DefaultReceiptConcurrency = 1

// MaxReceiptConcurrency caps the per-block receipt pool; it does not permit parallel height
// commits.
const MaxReceiptConcurrency = 8

// fetchReceipts owns a bounded batch of workers, never a goroutine per receipt.
// Each index is claimed once. Only its owner writes its result slice position;
// ordered cross-receipt invariants run after all workers have joined.
func (c *Client) fetchReceipts(parent context.Context, e *domain.Extraction, txs []transactionInfo, gasUsed uint64) (returned error) {
	started := time.Now()
	defer func() {
		outcome := "completed"
		if returned != nil {
			outcome = "failed"
			if parent.Err() != nil {
				outcome = "interrupted"
			}
		}
		c.metrics.Observe("fetch_receipts", outcome, time.Since(started))
	}()
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	e.Receipts = make([]domain.Receipt, len(txs))
	e.RawReceipts = make([][]byte, len(txs))
	used := make([]uint64, len(txs))
	reported := make([]uint64, len(txs))
	var next atomic.Uint64
	var group sync.WaitGroup
	var sizeMu sync.Mutex
	total := int64(len(e.RawBlock))
	if total > c.config.MaxSnapshotBytes {
		return ErrSnapshotTooLarge
	}
	for range min(c.config.ReceiptConcurrency, len(txs)) {
		group.Go(func() {
			for ctx.Err() == nil {
				index := next.Add(1) - 1
				if index >= uint64(len(txs)) {
					return
				}
				i := int(index)
				raw, err := c.call(ctx, "eth_getTransactionReceipt", []any{fmt.Sprintf("0x%x", txs[i].meta.Hash)})
				if err != nil {
					cancel(fmt.Errorf("fetch receipt %d: %w", i, err))
					return
				}
				sizeMu.Lock()
				fits := int64(len(raw)) <= c.config.MaxSnapshotBytes-total
				if fits {
					total += int64(len(raw))
				}
				sizeMu.Unlock()
				if !fits {
					cancel(ErrSnapshotTooLarge)
					return
				}
				if ctx.Err() != nil {
					return
				}
				receipt, gas, err := decodeReceipt(raw, e.Header, txs[i])
				if err != nil {
					cancel(fmt.Errorf("receipt %d: %w", i, err))
					return
				}
				o, _ := objectOf(raw)
				cumulative, _ := uintField(o, "cumulativeGasUsed")
				e.Receipts[i] = receipt
				e.RawReceipts[i] = raw
				used[i] = gas
				reported[i] = cumulative
			}
		})
	}
	group.Wait()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	var cumulative uint64
	for i := range txs {
		if used[i] > math.MaxUint64-cumulative {
			return invalid("cumulativeGasUsed")
		}
		cumulative += used[i]
		if cumulative != reported[i] {
			return invalid("cumulativeGasUsed")
		}
	}
	if cumulative != gasUsed {
		return invalid("gasUsed")
	}
	return parent.Err()
}
