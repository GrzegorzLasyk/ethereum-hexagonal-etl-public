# ADR 0004: Ordered extraction and provider-relative reorg recovery

- Status: Accepted; explicitly approved by the user for Stage 10 with preservation, atomicity, ancestry verification, concurrency and restart-safety conditions.
- Scope: Stage 10 scheduling, Extractor lineage recovery and operator replay/reindex.
- Related: ADR 0001, ADR 0002, ADR 0003 and Stage 10 in PLAN.md.

## Why a decision is required

ADR 0002 and Extractor migration 0002 retain the first accepted lineage:
`raw_blocks` has `UNIQUE (chain_id, block_number)`, the commit adapter rejects
competing hashes, and ordinary commits never rewind the checkpoint. ADR 0003
allows Loader to retain conflicting observations but constrains all canonical
statuses to `unknown`. Existing protobuf events carry immutable observations,
not canonical-chain selection/retraction commands.

Automatic recovery cannot be implemented by merely retrying a conflicting block.
Dropping the height uniqueness constraint alone would not define the active
lineage or safely fence a checkpoint rewind. Deleting the old block would break
retention and stable-event guarantees. A choice is needed about whether recovery
changes only Extractor's processing cursor or also the downstream canonical view.

## Accepted decision: recover Extractor's observed lineage

1. Keep all raw snapshots and Outbox events immutable, keyed by full block
   identity. A forward migration replaces height-only uniqueness with a separate
   Extractor-owned active-lineage mapping. Historical fork observations remain
   available and eligible for their original at-least-once handoff.
2. Treat the configured provider's current branch as the source for continuing
   extraction, not as consensus proof. Loader keeps every candidate with
   `canonical_status=unknown`; Stage 10 emits no canonicality/retraction events
   and does not alter ADR 0003's Loader policy.
3. Persist the contiguous active lineage and last committed cursor locally.
   Normal commits preserve source + Outbox + lineage + checkpoint atomicity.
   Add a monotonic revision/fencing token so work fetched before a rewind cannot
   later advance the replacement lineage, even when the same tip reappears.
4. Verify the checkpoint against the provider before continuation and during
   follow-head polling. On a conflicting hash/parent, inspect a bounded contiguous
   ancestry window, checking parent links and rechecking the sampled anchor/tip.
   Do not rewind based on missing blocks, timeouts or a moving/inconsistent view.
5. If a matching retained ancestor is found within the explicitly configured
   recovery depth and indexed range, commit a conditional audited rewind under
   the same per-chain coordination as normal writes. Preserve old observations
   and events; only the active mapping/cursor changes. Resume sequentially from
   the ancestor's child. An ambiguous rewind COMMIT is resolved by rereading its
   durable operation record and revision, never by an unconditional second rewind.
6. If no ancestor is available, the provider head regresses below the cursor, or
   the fork exceeds the configured limit/start anchor, stop automatic recovery
   visibly and require operator investigation. No silent bootstrap onto another
   fork, arbitrary rollback depth, deletion or cross-service database access.
7. Confirmation depth means required descendants: for head H and depth D, only
   heights <= H-D are eligible; H<D yields no eligible block. D=0 explicitly
   allows the current head. D is required configuration, not an invented safe
   Mainnet finality default. Shallower observations stay unpersisted and are
   revisited by polling. Previously accepted observations are never removed when
   confirmations decrease. Confirmation depth is not finality.
8. START_BLOCK is explicit; END_BLOCK is inclusive when supplied, otherwise
   extraction follows the head. One complete block is fetched at a time. A bounded
   number per scheduling cycle and configured RPC pacing limit acquisition.
   Pause new acquisition when outstanding local Outbox count/bytes reach configured
   limits or the configured database-size ceiling is reached. Failed Outbox rows
   count as outstanding; dispatch continues while acquisition is paused.
9. Replay means retrying a retained, exact event through its owning Outbox, with
   explicit bounded selection and an auditable new retry allowance. Keep its ID,
   bytes and original provenance. Fence old claims; never reset an attempt token
   in a way that lets a stale worker complete. Replayed accepted Inbox entries
   still deduplicate; replay is not permission to reset downstream processing.
10. Reindex means bounded, restartable re-observation of a requested historical
    range, preserving the first stored snapshot for an existing identity and
    recording new fork candidates with new identity-derived IDs. It must not
    silently replace the live extraction cursor or retransform completed input.
    A separate audited repair operation is required to change the live lineage.
    Changed transformation semantics or a destructive full rebuild require a
    separate version/rebuild decision, outside this Stage 10 interpretation.

This option demonstrates bounded automatic recovery of extraction after a reorg,
while keeping the pipeline an observation store. Queries cannot select a unique
canonical branch from Loader solely on successful processing or confirmations.

## Alternatives requiring a different approved scope

- **End-to-end canonical reconciliation:** retain all versions but add versioned,
  ordered reconciliation events and durable receiver handling so Loader can
  transition canonical/orphan states. Define authority, generations, stale-event
  fencing, missing-ancestor behavior and upgrade compatibility. This extends
  ADR 0003 and the protobuf contract; existing block events alone are insufficient.
- **Detect and halt, operator-directed recovery only:** keep conflict detection
  and require an explicitly reviewed bounded repair before continuing. This is
  more conservative but does not provide the recommended automatic recovery.

## Implemented persistence and operational boundary

Migration `extractor/0004_active_branch.sql` removes only height uniqueness from
immutable observations, backfills `active_blocks`, pins the original start and
adds checkpoint `revision`. Existing raw bytes, provenance, IDs and delivery
state are preserved. Stop older Extractor binaries before applying the migration:
old writers do not maintain the new active-branch invariant.

All acquisition, reindex and rewind writes share the per-chain transaction
advisory lock and use `synchronous_commit=on`. Normal advancement preserves the
revision; each rewind increments it. A previously active exact duplicate can
return success without mutation, but reactivating historical data requires the
current expected checkpoint/revision. This fences ABA races when an old tip
reappears. `rewinds` records the old revision/tip and target; the same request
resolves a lost COMMIT response without undoing later progress. Original start
cannot be changed by runtime updates. The configured recovery depth is 1–10000.

Reindex uses an explicit operation ID and inclusive range in `reindex_jobs`.
Each block observation, optional new Outbox intent and job cursor commit together;
the live checkpoint/branch is untouched. A changed branch within an unfinished
job stops it; a new operation ID starts a separately audited observation pass.
Replay selects exactly one Extractor event per invocation. `replays` binds an
operation ID to event ID and 1–1000 additional attempts. Attempts never decrease;
replay increments the fencing token, cancels the old lease and grants the new
allowance. Repeating the same operation ID is a no-op; changing its arguments is
an error. Old claims cannot finish the new attempt. Integer exhaustion fails
without mutation. Neither operation clears downstream Inbox state or changes
transformation versions; replay of an accepted event remains deduplicated.

Backpressure counts all non-delivered events for the configured chain, including
failed events and historical branches. Count/byte checks run before acquisition
and again under the chain lock before inserting a new observation. Database size
is an admission watermark, not a disk quota or reservation: PostgreSQL overhead,
WAL and other services' growth cannot be predicted or locked by this service.
Existing snapshots and events are never deleted to relieve pressure.

The scheduler reads one complete block at a time and bounds work per cycle.
No end means follow-head; an explicit end is inclusive. The required confirmation
depth has no implicit Mainnet safety default. Pending/shallow blocks are not
persisted. RPC retries remain bounded with backoff; cycle retries also have a
finite consecutive-error budget. Deep/unprovable recovery halts immediately;
missing provider data may be retried but never authorizes a rewind. Repeated
source failures halt with logs and retained durable progress. Dispatch continues
while acquisition waits for confirmations or capacity. A range completion stops
its process's dispatcher after joining in-flight calls; remaining intents are
retained for a separate dispatcher or restart.

This decision supersedes ADR 0002's first-lineage-only persistence restriction.
It does not supersede ADR 0003, introduce downstream reconciliation messages,
change transport/identity versions, or authorize a destructive rebuild. See the
[Stage 10 runbook](../architecture/extraction-stage10.md) for configuration,
recovery actions, tests and limits.
