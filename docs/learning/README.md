# Learning path

These guides explain the implemented ETL, rather than proposing another architecture.
Start from the [root README](../../README.md), run synthetic fixtures, then follow a
single event through the service guides and source. Commands are from the repository
root. Historical validation records are evidence for their recorded scope, not a
promise that every environment or future revision will pass.

| Order | Guide | Question to answer by reading code |
|---|---|---|
| 1 | [Hexagonal Go, ports and dependency inversion](hexagonal-go.md) | Why does `Process` know `Queue`, but not pgx? |
| 2 | [Ethereum data, checkpoints and reorgs](ethereum-and-reorgs.md) | Why is a height insufficient as an immutable key? |
| 3 | [Durable handoffs, gRPC and idempotency](durable-handoffs.md) | What remains durable when the ACK disappears? |
| 4 | [Transactions, locking and fencing](transactions-locking.md) | Why must an old worker not complete a reclaimed claim? |
| 5 | [Backpressure, concurrency and batching](backpressure-performance.md) | Why can adding workers reduce throughput? |
| 6 | [Testing PostgreSQL microservices](testing.md) | What does a fake prove, and what requires a real database? |

Use the [documentation inventory](inventory.md), [source index](source-index.md),
[Extractor](../services/extractor.md), [Transformer](../services/transformer.md)
and [Loader](../services/loader.md) guides as a reading map. The
[runtime reference](../reference/runtime-reference.md) retains detailed configuration
and command tables. Accepted [ADRs](../adr/0001-mvp-transport-and-storage.md) remain
the authority for architectural choices; guides do not amend them.
