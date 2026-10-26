# Stage 12.2: structured logging and event correlation

## Scope and architecture

The three service entry points, inbound workers and gRPC receivers use `slog`
JSON handlers. `internal/logevent` is a shared technical formatter used only by
adapters/composition roots. Each service owns its error classification adapter
and application result types. Domain and application packages do not import
logging infrastructure. No protobuf, database schema, business policy, ACK,
retry, readiness or metrics contract changes.

Processing/delivery results preserve the claimed event ID, source ID where the
existing envelope contains it, attempt and persisted failure code. Error results
retain this context. Index results include a bounded list (at most the configured
batch size) of confirmed commits and the current operation's identity. They
contain no source buffers. The worker emits every commit even when a later block
in the same cycle fails, and logs the returned result before handling shutdown.
Extractor's existing unknown-commit sentinel is exposed at its outbound port;
the PostgreSQL name remains an alias, preserving error identity and behavior.

## Fields

Every application record includes standard `time`, `level`, `msg` plus:

| Field | Meaning |
| --- | --- |
| `service` | `extractor-service`, `transformer-service`, `loader-service` |
| `operation` | Fixed vocabulary listed below |
| `event_id` | Existing raw/transformed v1 identity, never a regenerated ID |
| `source_event_id` | Existing raw source ID on transformed envelopes; null for raw events |
| `chain_id` | Canonical decimal string |
| `block_number` | Unsigned JSON integer; null when unknown (genesis is 0) |
| `block_hash` | Lowercase 32-byte hash with `0x` prefix; null before acquisition |
| `attempt` | Durable Inbox/Outbox claim number; null for non-claim operations or an unsuccessful claim |
| `outcome` | Local operation result, not consensus finality |
| `error_code` | Reviewed category; empty string for success |
| `duration_ms` | Elapsed local operation time in integer milliseconds |

The formatter recognizes the existing v1 event ID grammar and derives chain,
height and hash without deserializing payloads. Invalid IDs are omitted (`null`).
A source ID is emitted only when it matches the same block's raw event identity.
For a fetch failure before a hash is known, indexing reports the configured chain
and requested height; it never fabricates an event ID. A claim whose commit result
is unknown returns no claimed item, so its identity and attempt remain null.

Worker duration spans claim, processing/publication and the local state update.
Index/reindex duration spans one block iteration, including checkpoint/head
checks; each committed block retains its own duration. Recovery duration spans
that iteration and the ancestor search/rewind. Replay duration includes startup and its call; acceptance duration
spans its call; lifecycle stop duration spans the run. These are log values,
not metrics. Idle queue polls produce no records; indexing waiting/backpressure
records follow the existing poll interval.

## Operations and outcomes

| Operation | Typical outcomes |
| --- | --- |
| `configure`, `run` | `failed`, `started`, `stopped`, `interrupted` |
| `accept_inbox` | `accepted`, `already_accepted`, `interrupted`, `unknown` |
| `process_inbox` | `completed`, `retry`, `failed`, `interrupted`, `unknown` |
| `dispatch_outbox` | `delivered`, `retry`, `failed`, `interrupted`, `unknown` |
| `index_block`, `reindex_block` | `completed`, `waiting`, `backpressure`, `done`, `interrupted`, `unknown` |
| `replay_outbox` | `requeued`, `interrupted`, `unknown` |
| `reorg_recovery` | `rewound`, `interrupted`, `unknown` (old tip identity) |
| `rewind_checkpoint` | `rewound`, `interrupted`, `unknown` (verified ancestor identity) |

`accepted` and `already_accepted` attest to durable Inbox acceptance only. They
are logged before the transport returns its ACK, so the peer can still lose that
ACK. A later delivery retry retains the same event ID and increases its durable
attempt. The receiver then logs `already_accepted`. A sender cannot distinguish
an outage before acceptance from ACK loss; both may appear as `unavailable`.
Do not infer rollback or duplication of materialized data from transport failure.

`completed` is the local processing/storage boundary. `delivered` additionally
means a verified durable receiver ACK and successful local published-state update.
`failed` on queue operations means the existing retry/permanence policy recorded
a terminal failure. `interrupted` means no successful result was returned, not a
proven rollback. `unknown` with `commit_unknown` explicitly marks ambiguity; it
must not be followed by a compensating change based on the log alone.

Retry/backpressure records are WARN, unsuccessful operations with an error code
are ERROR, and other outcomes are INFO. Existing level configuration can suppress
records. Error categories include `commit_unknown`, `lease_lost`, `unavailable`,
`invalid_ack`, `invalid`, `incomplete`, `identity`, `unsupported`, `conflict`,
`oversize`, `output_too_large`, `transient`, `canceled`, `deadline_exceeded`,
`recovery_required`, `unstable_chain`, `checkpoint_conflict`, `block_conflict`,
`chain_mismatch`, `out_of_sequence`, `retry_deferred` and `invalid_configuration`.
Unknown errors use `operation_failed`. No classifier formats `err.Error()`.

## Following an observation

1. Find `extractor-service/index_block` by chain, height and hash. Its raw
   `event_id` stays unchanged during dispatch retries, operator replay and reorg.
2. Follow that ID into `transformer-service/accept_inbox` and `process_inbox`.
3. Find Transformer `dispatch_outbox` records whose `source_event_id` equals
   that raw ID. Their `event_id` is the existing transformed v1 ID.
4. Follow the transformed ID into Loader `accept_inbox` and `process_inbox`.
5. For recovery, inspect `reorg_recovery` (old tip) and `rewind_checkpoint`
   (verified retained ancestor), then the subsequent `index_block` records.
   Consult Extractor's existing durable rewind audit/revision for authoritative
   recovery state. Historical events remain valid and deliverable; Loader
   candidates still have `canonical_status=unknown` (ADRs 0003 and 0004).

Use the Compose command in the [README](../../README.md#structured-logging-and-correlation-stage-122).
Filter locally by `event_id` or `source_event_id` using a JSON-aware viewer.
Do not turn up raw RPC/SQL payload logging to troubleshoot missing progress.

## Redaction and limits

Only selected identity metadata and fixed program-defined values enter logs.
Raw blocks, transactions, receipts, normalized JSON, provider labels, RPC URLs,
connection strings, credentials and underlying errors are excluded, including
configuration/startup failures. Rejected gRPC metadata is not copied to logs.
The formatter is not a general sanitizer for arbitrary attributes: new callers
must continue supplying fixed service/operation/outcome/error vocabulary.

These logs are best-effort diagnostics, not transactional audit records or a
replacement for durable checkpoints/Inbox/Outbox. A crash, output failure or
filter can lose logs. Batch commit records are emitted when the cycle returns;
a crash inside a cycle can therefore leave durable data without its log. There
is no global trace ID, external collector, persistent log retention, metrics or
new observability dependency. Existing event metadata provides correlation, not
exactly-once delivery or consensus canonicality.

## Verification

Unit tests inspect decoded JSON for success, failures, shutdown after a durable
success, missing identity during claim ambiguity, lost ACK retry codes, ambiguous
completion/publication, indexing/reindexing and reorg context, and rejection of
secret-bearing error/metadata strings. Application tests separately assert that
result metadata survives real orchestration branches and does not alter recovery.

The Stage 11 fixture E2E inspects actual service logs after all failure scenarios:
all retained raw observations correlate through both hops to Loader completion;
retry and duplicate acceptance logs exist; replay and simulated reorg retain
identity. Existing PostgreSQL runners continue testing local atomicity, lost
commit responses, privilege isolation and recovery with real databases.
