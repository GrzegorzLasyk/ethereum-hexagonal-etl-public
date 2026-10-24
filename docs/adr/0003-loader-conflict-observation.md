# ADR 0003: Loader conflict observation without canonical-chain selection

- Status: Accepted; explicitly approved by the user for Stage 8.
- Scope: Loader candidate retention and local conflict observations only.
- Related: [ADR 0001](0001-mvp-transport-and-storage.md),
  [ADR 0002](0002-ethereum-source-data.md),
  [Stage 8](../../PLAN.md#stage-8--loader-inbox-and-canonical-data-persistence).

## Context

Loader durably accepts exact-content events, validates normalized JSON v1 and
atomically persists records keyed by full block identity. Finality and automatic
reorganisation recovery remain deferred. Stage 8 must nevertheless retain and
expose competing hashes and parent conflicts under out-of-order delivery.
Arrival order does not establish canonicality.

## Accepted decision

- Preserve both independently valid block versions when the same chain ID and
  block number have different hashes. Identity remains `(chain_id, number, hash)`;
  transaction, receipt and log keys/foreign keys retain that complete identity.
- All loaded versions have `canonical_status='unknown'`, including conflict-free,
  linked, competing and successfully processed versions. Stage 8 never promotes,
  finalizes or declares a version orphaned. A future explicit reconciliation
  mechanism must define any canonical-state transition.
- Record `height_conflict=true` on every version at a height with competing
  hashes, and expose this flag through queries and a partial index.
- Observe parent links separately: `unknown` if no previous-height observation
  exists (also at genesis), `linked` if any previous-height candidate matches
  the parent hash, `mismatch` if previous-height candidates exist but none match.
  A linked child of a conflicting parent still has unknown canonicality.
- Revisit the incoming height and its immediate children atomically when a
  candidate arrives. Child-before-parent delivery is supported. A later matching
  parent changes `mismatch` or `unknown` to `linked`; this updates an observation,
  not canonicality or source records. No recursive descendant propagation occurs.
- Domain functions determine observation states. The PostgreSQL adapter reads
  same-chain neighboring identities and persists domain results within the
  existing local completion transaction. A per-chain transaction advisory lock
  serializes completions, preventing concurrent forks from missing each other.
- Commit normalized records, affected observations and successful Inbox completion
  together. Rollback includes changes to older candidates' observation flags.
  Exact event retries are idempotent; conflicting content under the same event
  ID remains an error. Competing block hashes have different event IDs and are
  preserved instead of failed solely because of the competing hash.
- Never silently overwrite/delete source fields, transactions, receipts or logs.
  Runtime may update only Inbox state and the two observation fields. Canonical
  status is constrained to `unknown` and cannot be updated by the runtime role.

Successful Inbox processing means that records and observations are durable.
It never implies canonicality, finality or proof that the selected provider's
chain is correct. Consumers must not interpret loaded or linked as canonical.

## Extractor and Loader intentionally differ

At Stage 8 acceptance, Extractor rejected competing hashes or parent discontinuity and stopped
advancing its accepted extraction lineage (ADR 0002 / Stage 5). Loader preserves
conflicting observations delivered to its own Inbox. This does not change
Extractor's policy or introduce a route around it; tests submit controlled
fixtures locally. Stage 9 transmission is not implemented here.

## Migration and limits

The forward Loader migration `0003_conflict_observations.sql` adds observation
columns, narrow privileges and a query index. It initializes observations for
records already loaded by migration 0002's schema. Deployment must apply all
Loader migrations before running this version. The migration's data backfill
mirrors the documented rules; ongoing decisions belong to the pure domain.

Per-chain serialization favors correctness and simplicity over write throughput;
decoding and Inbox claims remain concurrent. Hash-lock collisions can only add
serialization. Observation queries inspect the arriving height and immediate
neighbors, not the entire chain. All source records and candidates remain retained.

No canonical promotion, confirmation depth, finality tracking, automatic fork
reconciliation, rollback/refetch, replay tooling, deletion, broker, outgoing
transmission or subsequent stage is added. These require later explicit work.

## Rejected alternative

Failing a competing candidate would make arrival order select the materialized
query dataset. The user chose retention with explicit observations instead.

## Stage 10 compatibility note

Accepted ADR 0004 now permits Extractor to retain competing observations and
recover its separate active branch. This does not change any Loader decision in
this ADR: every candidate remains `canonical_status=unknown`, and there are no
canonical-selection or retraction messages. The first-lineage restriction above
records the Stage 8 context, not the current Extractor persistence contract.
