# Backpressure, bounded concurrency and measured optimization

A fast producer is harmful if a stopped consumer can cause unlimited durable work
or decoded messages. This pipeline bounds several different resources; no single
number is a complete memory/disk quota.

## Admission from the network to the database

[Admission](../../internal/adapters/grpccontract/admission.go) wraps registered gRPC
handlers before request decoding. It reserves a maximum-message budget and request
slot, then releases them when the handler finishes. Connection limits bound idle
transport exposure. Reservations are conservative serialized-message allowances,
not a measurement of protobuf objects, JSON decoding or total process heap.

After envelope validation, `Store.Accept` serializes admission, checks an existing
ID for exact equality **before** capacity, then probes outstanding owning-schema
work. Transformer counts incomplete Inbox plus undelivered Outbox; Loader counts
incomplete Inbox. New events at capacity receive retryable overload, never an ACK.
Accepted identical events can still receive their durable duplicate ACK.

If Loader is down, Transformer Outbox stays outstanding, Transformer rejects new
input, Extractor Outbox grows to its own count/byte watermark, and acquisition
pauses. Dispatch continues. Failed work retains slots, so recovery sometimes needs
audited repair rather than a restart. No layer deletes source history to regain room.

## Receipt workers: parallel I/O, ordered result

[fetchReceipts](../../services/extractor-service/internal/adapters/outbound/jsonrpc/receipts.go)
starts at most `min(concurrency, transactionCount)` workers. An atomic index assigns
one transaction position to each task. Each worker writes only its own position;
a wait group joins every worker before cumulative gas/order validation. There is
no goroutine per receipt and no unbounded channel of tasks.

A cancellation cause records the first batch failure. The shared client performs
pacing, Retry-After cooldown, deadline checks and actual-attempt reservations;
concurrency does not multiply the request budget. Already-started requests may
have reached the server when cancellation occurs. Validation rejects an incomplete
batch and returns no committable extraction.

Defaults remain one receipt worker; configured 2–8 can overlap I/O within a block,
not independent height commits. Provider rate limits can erase any speedup, so
correct concurrent acquisition is not evidence of improved provider throughput.

## Worker pools and PostgreSQL pools

Inbox and Outbox loops have independent bounded worker counts. More workers can
hide I/O waits but compete for database connections and same-chain locks. Lease
length must accommodate processing deadlines; otherwise slow valid work is reclaimed.
A larger pool consumes database resources and does not resolve lock serialization.

The default consumer capacity is 128 events, two incoming slots, 16 connections,
130 MiB reserved message budget, pool maximum 12 and one worker/dispatcher.
A 65 MiB maximum gRPC message includes metadata/framing beyond the 64 MiB aggregate
payload. Lowered limits must remain mutually compatible. Container 2g memory and
`GOMEMLIMIT=1536MiB` leave headroom; Go's limit is a soft GC target, not a hard bound.
See the full [configuration constraints](../architecture/backpressure-stage14.md).

## pgx batching: fewer submissions, same statements

`recordBatch.Exec` queues existing INSERTs, flushing at the configured count (default
64, maximum 256) or estimated 1 MiB coalescing bound. Size 1 executes directly.
Each statement retains its own placeholders; 65535 is checked per statement,
not by summing all unrelated batch parameters. Oversized individual rows go alone.
No ORM, COPY schema or multi-row rewrite was introduced.

The batch belongs to one block transaction and is never shared between workers.
All results are consumed/closed; first SQL failure is preserved. A successful
submission is not a committed block. [Batch tests](../../services/loader-service/internal/adapters/outbound/postgres/batch_test.go)
check limits, and PostgreSQL tests inject a late failure to check rollback.

## Interpret evidence conservatively

[Phase 2](../architecture/validation-performance-stage14.md) measured batch64 at about
2.06x/2.24x single-worker direct-write throughput on the two larger authentic blocks,
but tiny synthetic blocks showed little benefit. More workers often increased CPU,
allocations and pool waiting. Batch256 did not improve the measured W1 result.
Keep the defaults; do not infer the cause of every slowdown from aggregate counters.

Reports gate throughput on complete integrity, record host CPU/RSS/allocations/GC,
client SQL timings, cluster WAL, pool waits, gRPC observations and sampled backlogs.
One-block runs cannot yield population tail percentiles; RSS is not simultaneous
whole-system memory; WAL is not exactly attributable to one service. Nineteen batch
calls for 1167 INSERTs are not nineteen measured wire round trips. Ordinary runtime
metrics omit some opt-in `perfbaseline` probes. Study the method before tuning.
