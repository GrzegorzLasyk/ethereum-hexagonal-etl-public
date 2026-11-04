# Ethereum source data, checkpoints and reorganizations

A block contains ordered transactions. Each mined transaction has a receipt
(execution outcome, gas and logs). Logs are emitted execution data, not application
log messages. A block RPC response with full transactions still does not contain
all receipts; Extractor explicitly fetches one receipt per transaction.

## Data distinctions that matter

- JSON-RPC quantities are hex integers; normalized `Quantity` values are canonical
  decimal strings. Floating point would lose large integer precision.
- A missing field, explicit null and present zero are different. Transformer and
  Loader `Optional[T]` preserve those distinctions. A null recipient can mean
  contract creation; it is not the zero address.
- Historical receipts can have a state root instead of a success/failure status.
  A failed transaction with a valid receipt is still valid ETL input.
- Transaction types require different fee/signature/access-list/blob/authorization
  fields. Unknown raw source fields remain in Extractor bytes, but are not all
  automatically promoted to normalized columns. No blob sidecars, ABI decoding or
  internal-call traces are collected.
- Raw preservation means exact successful block/receipt JSON **result** bytes.
  The normal raw event does not preserve all HTTP headers or JSON-RPC request IDs.
  Captured offline artifacts additionally preserve the selected RPC responses.

Read [ADR 0002](../adr/0002-ethereum-source-data.md), the RPC
[decoder](../../services/extractor-service/internal/adapters/outbound/jsonrpc/decode.go)
and Transformer [value model](../../services/transformer-service/internal/domain/model.go).

## Identity and checkpoint

`(chain_id, block_number, block_hash)` identifies an observation. Height alone
cannot distinguish competing blocks. Stable event IDs include this full identity.
A checkpoint is the last **locally committed** active observation, not a promise
that Loader is complete or the block is finalized.

`Index.Cycle` rereads the checkpoint, observes the provider head, checks the stored
tip against the provider, applies `eligible = head - confirmationDepth`, checks
capacity, fetches the next block, rechecks the sampled block/anchor and commits.
No subtraction occurs when the head is shallower than the confirmation depth.
Work per cycle is bounded; heights commit sequentially even with concurrent receipts.

## A small reorg example

Educational example, not a captured Mainnet event:

```text
retained active branch: A100 -> A101 -> A102
provider now reports:  A100 -> B101 -> B102
```

Within the configured recovery depth, the use case walks parent-linked headers and
compares them with retained active history. It rechecks anchors to reject an unstable
provider view. If A100 is a provable common ancestor, `Store.Rewind` atomically:

1. records the expected old checkpoint/revision and chosen ancestor;
2. removes active membership above A100, not raw snapshots or Outbox rows;
3. moves the checkpoint to A100 and increments its revision.

Sequential acquisition then observes B101/B102. A101/A102 and their events remain
retrievable and deliverable. An old writer cannot advance under the previous
revision. An ambiguous rewind commit is resolved through its retained audit record.

Missing ancestry, an out-of-depth fork, a regressed provider head or inconsistent
anchors stops automatic recovery. A timeout is never evidence for deleting a branch.
See [Index.reconcile](../../services/extractor-service/internal/application/usecases/index.go)
and [Store.Rewind](../../services/extractor-service/internal/adapters/outbound/postgres/branch.go).

## Why Loader canonicality stays unknown

Loader receives immutable observations, not authoritative branch-selection messages.
It records competing-height and neighboring-parent facts. Child-before-parent
arrival is supported, and observations may change when a parent appears, but source
records are not overwritten. `canonical_status=unknown` is enforced even when a
parent links and there is no local conflict. Extractor's chosen provider branch and
confirmation depth establish neither consensus finality nor canonical truth.

This deliberate limit is [ADR 0003](../adr/0003-loader-conflict-observation.md), not
an unfinished flag to flip manually. [ADR 0004](../adr/0004-ordered-extraction-reorg-recovery.md)
changes Extractor recovery while preserving that downstream rule.

Authentic fixtures cover types 0/1/2/3, failed receipts and logs. Type 4 and contract
creation are supported by deterministic model tests but not evidenced by those
three authentic captures. Noncontiguous fixture heights use separate database runs;
advancing one sequential checkpoint over their gaps would invent history.
