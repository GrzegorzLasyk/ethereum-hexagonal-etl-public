# ADR 0002: Ethereum source data and single-block extraction boundary

- Status: Accepted for the Stage 3 resource scope and contracts below; deployment choices remain open.
- Scope: Extractor domain and application only. No real RPC, database, or transport adapters.
- Related: [ADR 0001](0001-mvp-transport-and-storage.md), [plan](../../PLAN.md), [instructions](../../AGENTS.md).

## Decision and ownership

Collect mined block headers, full transaction objects, every transaction receipt, and every receipt log. Exclude traces, internal-call reconstruction, ABI decoding, separate state/balance queries, and blob sidecars. Preserve additional fields returned within these resources without silently dropping them. Included withdrawals or uncle hashes remain part of the raw block; separate withdrawal/uncle querying and normalization are outside this stage.

Extractor owns a retrievable source snapshot, not the Transformer's normalized query model. Its pure domain types describe block identity, transaction/receipt membership, log identity, provenance, and extraction progress. The full source payload is preserved as opaque bytes alongside these typed metadata. There are no JSON tags, RPC response structs, SQL types, or generated protobuf types in the domain. Source adapters must decode and validate transport DTOs outside domain/application and construct these domain values; Transformer will later normalize the approved fields into its own model.

## Source methods and fields

The future source adapter must first verify `eth_chainId` against the explicitly configured chain. Fetch a numbered mined block with `eth_getBlockByNumber(number, true)`; `true` requests full transaction objects. Fetch receipts with `eth_getTransactionReceipt(hash)` for each transaction. `eth_getBlockReceipts` is an optional optimization only after verifying provider support and identical completeness checks. Logs come from receipts; no independent `eth_getLogs` scan is needed for this scope. A block response alone does not include receipts. Null results and provider/RPC errors are failures, never empty blocks or successful partial extraction. See the [Ethereum JSON-RPC reference](https://ethereum.org/en/developers/docs/apis/json-rpc/).

The acquisition contract preserves the following named fields, with fork/type-dependent fields required when applicable rather than fabricated for historical blocks:

| Resource | Fields to retain |
| --- | --- |
| Block/header | `number`, `hash`, `parentHash`, `timestamp`, `nonce`, `sha3Uncles`, `logsBloom`, `transactionsRoot`, `stateRoot`, `receiptsRoot`, `miner`, `difficulty`, `extraData`, `gasLimit`, `gasUsed`, `mixHash`; block `size`, full `transactions`, `uncles`; retain `totalDifficulty` when supplied |
| Fork-dependent block data | `baseFeePerGas`, `withdrawalsRoot`, `withdrawals`, `blobGasUsed`, `excessBlobGas`, `parentBeaconBlockRoot`, `requestsHash`, and additional returned header fields |
| Full transaction | `hash`, `blockHash`, `blockNumber`, `transactionIndex`, `from`, nullable `to`, `nonce`, `value`, `gas`, `input`, signature `r`/`s` and applicable `v`/`yParity`; applicable `type`, `chainId`, `gasPrice`, `maxFeePerGas`, `maxPriorityFeePerGas`, `accessList`, `maxFeePerBlobGas`, `blobVersionedHashes`, `authorizationList` |
| Receipt | `transactionHash`, `transactionIndex`, `blockHash`, `blockNumber`, `from`, nullable `to` and `contractAddress`, `cumulativeGasUsed`, `gasUsed`, `logsBloom`, `logs`; applicable `type`, `effectiveGasPrice`, `blobGasUsed`, `blobGasPrice`, and outcome as described below |
| Log | `blockHash`, `blockNumber`, `transactionHash`, `transactionIndex`, `logIndex`, `address`, `topics`, `data`; retain `removed` and `blockTimestamp` when supplied |

These resource definitions follow the execution API schemas for [blocks](https://github.com/ethereum/execution-apis/blob/main/src/schemas/block.yaml), [transactions](https://github.com/ethereum/execution-apis/blob/main/src/schemas/transaction.yaml), and [receipts/logs](https://github.com/ethereum/execution-apis/blob/main/src/schemas/receipt.yaml). Preserve optional, absent, and null distinctions. In particular, contract creation is not the zero recipient address, and historical receipts have a post-state `root` instead of the post-Byzantium `status` (0 or 1). A failed transaction with a valid receipt is still valid extraction input.

Do not use floating point or truncate large quantities. Stage 3 chain IDs use canonical nonnegative decimal strings (including chain 0), avoiding an implicit uint64 limit; adapter conversion from RPC hex belongs at the boundary. Block numbers and indexes are uint64 in the application contract; reject overflow explicitly in the adapter. Hashes and addresses are fixed-width byte values. The adapter must reject missing/null required mined-block identity fields rather than converting them to zero values.

Raw storage consists of the exact successful block result bytes (containing full transactions) and one exact receipt result per transaction in block order. JSON-RPC envelopes, request IDs, and provider credentials are not domain entities. The adapter must check JSON shape, required fields, quantities, and consistency between the raw results and the typed metadata; Stage 3 cannot prove the contents of opaque bytes. Do not reserialize a reduced projection and call it the original source snapshot.

## Domain validation

The domain checks a valid chain ID and nonzero block/transaction hashes; a zero parent is allowed only at genesis. Transactions must have unique hashes and contiguous indexes in block order. Every transaction and receipt must reference the same chain, block number/hash, and transaction position. There must be exactly one nonempty raw receipt and one receipt metadata record per transaction, in the same order.

All receipt logs must refer to their enclosing block/transaction. Their global log indexes must be contiguous across receipts in transaction order, their topic count must not exceed four, and removed logs invalidate the snapshot. Explicit empty transaction, receipt, and log collections are valid; missing collections are not silently treated as complete. Source provenance includes a non-secret provider label, chain/block identity, transaction identifiers, and a nonzero extraction time. A nonempty raw block is required even for an empty block.

This validates internal source consistency, not cryptographic header hashes, signatures, trie roots, or consensus canonicality. Provider selection/trust and finality are separate decisions. The domain rejects conflicts; it does not repair reorganizations.

## ExtractBlock and persistence contract

The use case is configured with one explicit chain ID and starting height. Each request repeats the intended chain ID and names one block number. It checks cancellation and request identity, verifies source chain identity, reads the current extraction checkpoint, fetches and validates the complete source snapshot, and calls one atomic persistence operation. It does not publish an event, retry internally, poll the head, or write a checkpoint separately.

The checkpoint stores the last durably committed block identity, not a next-height counter or downstream completion flag. With no checkpoint, only the configured starting block is accepted. With a checkpoint, accept its immediate child or a retry of that same height. Reject gaps and older requests; historical replay and range scheduling belong to Stage 10. Comparing the child's parent hash with the checkpoint detects a continuity conflict. A different hash at the checkpoint height is a conflict, not an overwrite. Stop and surface the conflict without selecting a replacement canonical chain.

Persistence receives the complete extraction and the expected previous checkpoint. Its contract requires an atomic local transaction that compares the expected checkpoint, stores the original source snapshot, records one outbox intent with the stable event ID, and advances progress together. The future adapter must recheck this boundary under concurrency. A successful duplicate of an already committed block returns `AlreadyStored` and retains the first snapshot/event; it must not overwrite provenance or enqueue another event. A concurrent checkpoint change yields a checkpoint conflict unless the exact block has already committed. No cross-service transaction or downstream ACK is involved.

The stable event identity is `raw-block/v1/<decimal-chain-id>/<decimal-number>/<lowercase-hash>` (hash without an RPC prefix). It depends on source block identity, not provider, extraction time, retry count, or random IDs. Future event versions/reconciliation commands require explicitly versioned semantics rather than silently changing this key.

## Failure and cancellation semantics

- Invalid requests, mismatched chains, unavailable/partial source data, malformed metadata, and parent/hash conflicts must not invoke persistence.
- Preserve wrapped error identity with `%w`, including source timeouts and `context.Canceled` / `context.DeadlineExceeded`.
- Propagate the same context to every I/O port and check cancellation between calls and before persistence.
- A persistence error must not be reported as successful extraction. The caller retries the same block/event identity after inspecting or rereading progress.
- Cancellation or connection failure during a real commit can have an ambiguous outcome. An adapter must not claim rollback unless known; idempotent retry resolves a commit whose success response was lost.
- If persistence reports success, return success even if cancellation arrives immediately afterwards: the durable boundary has already been crossed.

Stage 3 fakes exercise these contracts in memory. They are not production persistence, do not create migrations, and are never wired into the executable. Database atomicity and durable recovery remain to be tested with a real adapter in Stage 5.

## Alternatives and unresolved decisions

Returning only hashes would omit transaction bodies; fetching blocks without receipts would lose execution outcomes and logs. Fully normalizing every field inside Extractor would duplicate the Transformer's responsibility. Keeping opaque data without any typed membership checks would permit incomplete/mixed-block handoff. The chosen split preserves source data and validates extraction identity without coupling service domains.

The development network/provider, actual RPC URL and history support, payload byte limits and oversize policy, batching/rate limits, retry/backoff values, confirmation depth/finality and reorg reconciliation policy remain open. No defaults for these choices are selected here. Decide and document them before their dependent adapters/stages. The raw source snapshot defined here does not decide the later gRPC envelope or payload-versus-reference contract. Traces remain excluded.
