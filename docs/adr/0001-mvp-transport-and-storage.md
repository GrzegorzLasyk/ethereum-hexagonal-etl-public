# ADR 0001: MVP transport, storage, and reliable service handoff

- Status: Accepted for the approved MVP decisions below; open questions remain deferred.
- Date: 2026-10-21
- Sources: [AGENTS.md](../../AGENTS.md) and [PLAN.md](../../PLAN.md).
- Scope: Stage 0 records architecture only. It does not implement services or infrastructure.

## Context

The project is an educational Ethereum ETL pipeline developed in small, reviewable stages. It needs independently bounded extraction, transformation, and loading, with recoverable progress and reliable handoff across crashes and downstream outages. The MVP should demonstrate these guarantees without operating a separate message broker or introducing an analytics platform.

## Decisions

### Services, module, and dependency direction

Use Go and the existing root module, `github.com/GrzegorzLasyk/ethereum-hexagonal-etl`. The current `go.mod` declares Go `1.27.3`; this ADR does not change it. Keep one repository and one module, with `extractor-service`, `transformer-service`, and `loader-service` under `services/` when implementation begins.

Apply Hexagonal Architecture independently to every service:

- Domain contains pure entities, value objects, invariants, and errors.
- Application use cases orchestrate behavior through small, consumer-oriented inbound and outbound ports.
- Inbound adapters translate external requests and worker triggers into use-case calls.
- Outbound adapters implement Ethereum JSON-RPC, PostgreSQL, and transport access.
- Each executable entry point is an explicit composition root wiring adapters to ports.

Dependencies point inward. Domain and application packages do not import adapters, database clients, JSON-RPC clients, or generated protobuf types. Keep transport and storage DTOs separate from domain entities. Share only necessary technical contracts or utilities, not a shared domain model. Introduce packages when they have an implementation purpose rather than creating empty scaffolding.

### Transport and contracts

Use gRPC with Protocol Buffers for Extractor-to-Transformer and Transformer-to-Loader communication. Version contracts under `proto/etl/v1/` in the planned contract stage, with reproducible, pinned code generation. The planned handoffs are `SubmitRawBlock` and `SubmitTransformedBlock`.

gRPC transports an event payload. It is **not a durable broker or queue** and does not supply persistent buffering, replay, or exactly-once delivery. Durability belongs to service-owned PostgreSQL storage and application processing, not to the lifetime of an RPC connection.

Contracts must carry stable event IDs and chain/block provenance and document payload bounds, error handling, and durable-acceptance semantics. The exact payload representation and size limits remain open; choosing gRPC does not settle them.

### PostgreSQL ownership and isolation

Use one PostgreSQL instance and one database named `ethereum_etl`, with three service-owned schemas:

| Service | Schema | Owned persistent data |
| --- | --- | --- |
| Extractor | `extractor` | Retrievable raw Ethereum source data, provenance, progress/checkpoints, and outgoing events |
| Transformer | `transformer` | Durable inbox, processing and failure state, transformation results/state, and outgoing transformed events |
| Loader | `loader` | Durable inbox and processing state, plus idempotently persisted queryable blocks, transactions, receipts, and logs as defined by the approved dataset |

Give each service separate database roles, permissions, and migrations. Runtime credentials must be limited to the owning service's schema; use dedicated migration credentials for schema changes rather than broad runtime privileges. Services must not read or write another service's schema, including for raw-data lookup or status checks. Inter-service access goes through gRPC.

Use atomic **local** PostgreSQL transactions for each service's consistency boundaries. Do not use cross-service database transactions, even though the schemas share a database.

### Durable acceptance and processing

Use transactional outbox/inbox tables, Go background workers, retries with backoff, and idempotent consumers. Delivery semantics are **at least once**.

1. Extractor atomically commits the raw source data, corresponding outbox event, and any checkpoint advancement in its own transaction. A failed write must not advance progress or silently skip a block. The checkpoint means extraction and handoff intent are durable; it does not mean downstream processing is complete.
2. The sender dispatches its committed outbox event over gRPC. It retains the event and retries until durable acceptance is acknowledged. Marking an event as delivered requires that acknowledgement.
3. The receiver validates the request and commits the accepted event into its own durable inbox **before returning a successful gRPC ACK**. An ACK means durably accepted, not necessarily transformed or loaded. A receiver without working inbox persistence must return an appropriate error, never a simulated success.
4. Stable event IDs and unique inbox keys deduplicate repeated deliveries. Retries reuse the event ID. A duplicate of an already durably accepted event can be acknowledged without scheduling duplicate logical work.
5. Transformer processes accepted input asynchronously. It atomically commits transformation results/state, the corresponding outgoing event, and the relevant inbox completion state, so a crash cannot leave completed input without a durable outgoing handoff.
6. Loader processes its inbox asynchronously and commits idempotent target writes and the corresponding inbox completion state in a local transaction.

Workers need bounded concurrency, safe claims, crash recovery, deadlines, context cancellation, graceful shutdown, and observable pending/failed work. Use bounded retry attempts with backoff and jitter for transient failures. Exhausting automatic retries must retain unacknowledged or unprocessed work in an explicit failure state for recovery; it must not silently discard it or report successful delivery. Detailed retry, recovery, and retention policies belong to their implementation stages.

### Crash and retry semantics

| Failure boundary | Required behavior |
| --- | --- |
| Extractor crashes before its transaction commits | No partial checkpoint advancement; restart can fetch and persist the block again. |
| Sender crashes after local commit but before sending | The outbox preserves pending work for restart. |
| Receiver fails before inbox commit | No successful ACK; the sender retains and retries the event. |
| Inbox commits but the ACK is lost, times out, or the sender crashes before recording it | Retry with the same event ID; receiver deduplication prevents duplicate logical processing. |
| Receiver crashes after ACK but before processing | The committed inbox remains available for recovery. |
| A processing transaction fails | Roll back its local effects and retain recoverable input and explicit failure/retry state. |
| A downstream service is unavailable | Pending work stays durable; dispatch resumes after recovery, subject to capacity and retry policy. |

Idempotency makes repeated processing safe; it does not turn the transport into exactly-once delivery. Track downstream completion separately from upstream extraction progress. Do not assume ordering or permanent availability merely because requests use gRPC.

### Ethereum correctness boundaries

Use an explicitly configured Ethereum-compatible JSON-RPC provider, chain ID, and starting block. Range limits and follow-head behavior must be configurable. Preserve source provenance, including chain ID, block number/hash, transaction identifiers, extraction time, and relevant provider metadata; never mix chains silently.

Define exact source methods and fields before extraction implementation. Full blocks with transactions plus separately retrieved receipts/logs are the recommended dataset, pending the source-data ADR. A block response does not automatically contain all receipts. Traces are deferred from the MVP.

Identify blocks using `(chain_id, number, hash)` and model canonical status independently. Block number alone is not an immutable identity. Detect parent-hash mismatches and different hashes at an observed height; stop naive forward progress and use a documented recovery/reconciliation path. Confirmation depth must be configurable, but its value and the exact finality/reorg policy remain unresolved. Do not claim irreversible finality from a confirmation count.

Provider limits, timeouts, cancellation, rate limiting, supported batching, partial responses, unavailable data, and missing receipts require explicit handling. Ordinary unit tests use fixtures or mocks, not live Ethereum access.

### Deployment and scope

Keep the initial deployment simple. Introduce PostgreSQL through Docker Compose in Stage 2, service-specific persistence and integration tests in their planned stages, and the full local three-service environment with deterministic end-to-end tests in Stage 11. GoLand and Codex are development tools; GoLand MCP is optional and is not a runtime dependency.

Do not introduce Kafka, RabbitMQ, another standalone message broker, Kubernetes, or a KYT Analyzer in the MVP. Dashboards, wallet profiling, risk scoring, complex analytics, multi-chain support, and other product extensions are outside this scope. Configuration and secrets must remain explicit, with secrets outside committed files.

## Rationale

Three independently bounded hexagonal services make ownership and dependency direction visible while allowing domain and application behavior to be tested without infrastructure. One Go module keeps builds and technical contracts manageable during incremental development.

gRPC and Protocol Buffers provide explicit, versioned service contracts. PostgreSQL-backed outbox/inbox storage makes the intended handoff durable alongside business state using local transactions. A committed inbox separates durable acceptance from potentially slow processing and allows a receiver to continue after the original RPC has ended.

One PostgreSQL instance reduces local operational overhead. Separate schemas, roles, and migrations preserve service ownership and prevent direct database coupling. Omitting a broker and Kubernetes keeps the MVP focused on ETL correctness and recovery, while ports leave room for later infrastructure changes.

## Consequences and limitations

- The team must implement and test dispatching, deduplication, worker claims, retries, poison-event handling, recovery, and replay. gRPC does not provide these facilities.
- Completion is asynchronous and eventually consistent across services. An upstream checkpoint or RPC ACK is not evidence that queryable Loader data is ready.
- Duplicate delivery is expected. Idempotency and transactional completion must cover crash and timeout boundaries, not just successful requests.
- One PostgreSQL instance is a shared availability, maintenance, and resource bottleneck. Schema isolation is a permission boundary, not independent infrastructure or resource isolation.
- Downstream outages increase durable backlog. Storage capacity, backpressure, retention, deduplication history, and recovery procedures need explicit policies; durability does not imply unlimited buffering or guaranteed progress through permanent errors.
- Large events can exceed transport or storage limits. The payload decision must define rejection/recovery behavior without silent truncation.
- Durable delivery does not itself solve chain reorganizations, finality, conflicting data, or out-of-order arrival. Those require explicit domain/application policies and tests.
- Network authentication/TLS and deployment security details are not selected by this ADR; they require review before exposure beyond the intended local environment.

## Alternatives considered

| Alternative | Why it is not the approved MVP choice |
| --- | --- |
| Direct gRPC calls with only in-memory retries | Lose handoff intent or accepted work across crashes; do not meet durable recovery requirements. |
| Treating ACK as completion of the entire downstream pipeline | Couples request duration and availability to downstream processing; conflicts with durable-acceptance semantics. |
| Kafka or RabbitMQ from the start | Adds deployment and operational concepts before the ETL guarantees are established; requires explicit approval as a later extension. |
| HTTP/JSON between services | A possible transport adapter, but gRPC and Protocol Buffers are already approved for explicit service contracts. |
| Shared tables or direct cross-schema reads | Bypass service ports and ownership and couple schema evolution across services. |
| Separate PostgreSQL instances/databases per service immediately | Stronger infrastructure isolation at greater operational cost; defer until requirements justify it. |
| Cross-service transactions or an exactly-once claim | Conflict with independent local consistency boundaries and ambiguous network outcomes; use at-least-once delivery and idempotent effects. |
| One monolithic ETL service or a shared domain model | Does not preserve the approved three-service educational and ownership boundaries. |
| Multiple Go modules or Kubernetes immediately | Adds build/deployment coordination without a demonstrated MVP need. |

## Open questions and decision points

These are unresolved details, not permission to replace approved decisions. Record their resolution in subsequent ADRs before implementing infrastructure that depends on them.

| Question | Options or constraints | Decision needed by |
| --- | --- | --- |
| Development network and provider | Choose an explicit network/provider and verify chain ID; examples use non-secret placeholders. | Source-data design and JSON-RPC adapter, Stages 3–4 |
| Exact RPC resources and fields | Recommended blocks with full transactions plus receipts/logs; define method support and fallback receipt retrieval; exclude traces. | Source-data ADR, Stage 3 |
| Confirmation/finality and reorg policy | Select confirmation depth, canonical-state rules, and recovery/reconciliation behavior; distinguish confirmations from finality. | Identity/checkpoint design in Stages 3–5, with continuous recovery implemented in Stage 10 |
| Payload representation and size limits | Recommended bounded raw JSON envelope in the event versus references retrieved from Extractor over gRPC; persist the exact accepted message and define oversized-input behavior. | Dependent persistence/contract design, Stages 5–6 |
| Worker, retry, replay, and retention details | Define attempt limits, failure recovery, claims, deduplication lifetime, backlog bounds, and cleanup without losing pending work. | Relevant persistence/worker stages, with operational review in Stage 12 |

## Future extension options

After MVP stability and explicit approval, introduce a Kafka or RabbitMQ adapter behind an outbound port. Reassess acknowledgement, consumer persistence, offsets, ordering, replay, and delivery semantics; changing transport alone does not eliminate consistency requirements. Keep broker types out of domain and use cases.

Other separately scoped options include independent service databases, Kubernetes and autoscaling, additional Ethereum resources such as traces or decoded token events, richer safe/finalized-head and multi-network support, and query/reporting APIs. A future KYT Analyzer must have its own domain/service boundary and is not part of this MVP.

Operational extensions may include OpenTelemetry, dashboards, dead-letter recovery tooling, payload archival/object storage, partitioning, and retention automation. None is authorized for implementation by this ADR.

## Stage 0 repository review

The current `AGENTS.md` and `PLAN.md` agree on the approved architecture; no contradiction requires changing `AGENTS.md`. Recommendations and unresolved items in the plan remain distinct from accepted decisions here.

The root `main.go` only prints `Hello` and contains no ETL behavior. Keep it unchanged in Stage 0. Recommend removing it in Stage 1 after the three service entry points are established and build successfully, so the root placeholder does not imply a fourth executable with a pipeline role.

Stage 0 adds this ADR only. Existing local edits to `PLAN.md`, private-file exclusions, Go configuration, dependencies, and entry points are preserved. Recording decisions does not establish runtime guarantees; those require the implementation and verification stages in the plan.
