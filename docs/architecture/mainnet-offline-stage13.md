# Stage 13.3C — Authentic Mainnet offline ETL regression

The local opt-in regression reuses Stage 9's PostgreSQL/gRPC harness. It reads
existing capture artifacts; it never captures data or contacts Ethereum RPC.
The selection manifest and capture verification remain the Stage 13.3A/B formats.
No schema, migration, protobuf, business behavior or canonicality rule changes.

## Run from the repository root

Prerequisites: the declared Go toolchain, Docker Desktop with Compose, and the
three previously captured private artifacts. Do not source an RPC credential
file. The runner removes inherited `ETHEREUM_*`, service and Compose settings.

```sh
go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b
```

Before starting any database it runs these existing offline verifiers:

```sh
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json historical-smoke .mainnet/fixtures/stage13-3b/1000000.json
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json pectra-blob-dynamic-fee .mainnet/fixtures/stage13-3b/22431084.json
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json london-access-list .mainnet/fixtures/stage13-3b/12965000.json
```

Verification checks the candidate against the independent pinned manifest,
SHA-256 for every raw result, chain/block identity, transaction/receipt/log
identity, order and completeness, recorded coverage and endpoint redaction.
The complete artifact SHA-256 is also pinned for the lifetime of the regression
and checked again by the seed and assertion processes. Missing, changed,
malformed or incomplete files fail; they are never replaced with synthetic data.

Each noncontiguous height gets a **new random `ethereum-stage13c-*` Compose
project**, PostgreSQL volume and seven fresh role credentials. Existing fixture
E2E/Mainnet volumes are not mounted. The checkpoint starts and ends at the chosen
height, revision zero. No intervening blocks or parent observations are invented.
All three artifacts must verify before the first database starts; any subsequent
seed/handoff failure stops the sequence. Each capture is replayed separately.

Each project is stopped at completion/failure and **always retained**, including
its private mode-0600 configuration (the path is printed, contents are not).
No automatic volume deletion takes place, even if `ETL_TEST_RETAIN=0` was set.
Use the printed project/config with `docker compose --env-file <file> -p
<project> -f deployments/compose/compose.yaml up -d postgres` for inspection.
Keep those private configuration files out of Git. Retention consumes local disk;
review backups and explicitly manage disposable resources yourself. Repeated retained
projects also consume Docker network address pools. If Docker reports that all
predefined pools are fully subnetted, explicitly review and run `down` without
`--volumes` for a stopped disposable test project to release its containers and
network while preserving the database volume and private configuration. Do not
use Docker prune or delete unrelated projects.

## Replay and assertions

The Extractor-owned fixture adapter implements its existing `BlockSource` port.
It accepts only one verified height and feeds **unchanged successful JSON-RPC
result bytes** through the production RPC decoder using an in-memory HTTP
transport. There is no network transport fallback, endpoint configuration or
head discovery. It preserves original provider/acquisition provenance.

The seed invokes the production extraction use case/store twice. It checks exact
raw block and receipt bytes, provenance, one active block, isolated checkpoint,
one pending Outbox event and idempotent duplicate extraction. Service binaries
then run the production dispatchers, gRPC receivers, workers, normalization and
persistence with their own runtime roles. The harness checks:

- Durable receiver Inbox before each ACK; both transactional Outboxes delivered.
- Transformer/Loader initial outages, lost ACKs, sender crashes, lease recovery,
  duplicate `ALREADY_ACCEPTED` ACKs and identical entire protobuf requests.
- Original bytes/provenance in Extractor and Transformer, unchanged normalized
  payload in Loader Inbox, stable raw/transformed event IDs and `source_event_id`.
- Exactly one processed snapshot per service and expected normalized row counts.
- Every approved normalized block, transaction, receipt and log source field
  against original JSON, using an independent test oracle. Integer comparisons
  retain full 256-bit precision; nullable values and optional presence metadata,
  arrays, indexes, order, topics and data are checked.
- `canonical_status=unknown`, no height conflict, unknown parent linkage, and
  no synthetic parent/history. Each service role is denied cross-schema reads.
- Graceful final shutdown. Harness, seed and service binaries use race detection.

The source's extra fields remain available in exact raw JSON. The test does not
claim that unmodeled source extensions (for example withdrawals bodies) become
new normalized columns. Existing Stage 7/8 payload scope is unchanged.

## Verified local coverage

| Height | Transactions / receipts | Types | Failed receipts | Historical root receipts | Logs | Top-level contract creation |
| --- | --- | --- | --- | --- | --- | --- |
| 1000000 | 2 / 2 | 0: 2 | not applicable: pre-Byzantium root | 2 | 1 | 0 |
| 22431084 | 95 / 95 | 0: 10, 2: 83, 3: 2 | 3 | 0 | 233 | 0 |
| 12965000 | 259 / 259 | 0: 258, 1: 1 | 32 | 0 | 648 | 0 |

These counts were read from the verified local artifacts, not inferred from
block age. Type 4 and top-level contract creation remain **missing coverage**.
Internal contract creation is not asserted: these fixtures have no traces.
There are 356 transactions/receipts and 882 logs in total.

Artifact file SHA-256 (distinct from hashes of raw results inside each artifact):

| Height | SHA-256 |
| --- | --- |
| 1000000 | `8ef602ed4635ccca386ddd226464d23dce8be424c2c8912c34279bf7946fc734` |
| 22431084 | `fa6e3b12876cc67946f67ed2c08a2f19de18e01e1272513c9b110807849e7b71` |
| 12965000 | `ab870cfbd26aaf80c39b0e6c4f8a05d1808e5cb8107a0ca2630d97cdf60d930e` |

## Ordinary CI and reproducibility

`go test ./...` runs synthetic replay and oracle regression tests, without Docker,
private artifacts or provider credentials. The usual Stage 9 seed/handoff and
Stage 11 Compose fixture runners retain their synthetic defaults. The explicit
local mode refuses missing inputs instead of skipping. Required integration
PASS markers are checked; a skipped test cannot make this runner green.

The authentic files remain Git-ignored/private. A clean public checkout therefore
cannot reproduce this particular real-data dataset by itself. Before enabling it
in hosted CI, separately review and publish immutable credential-free artifacts
(or an access-controlled immutable bundle) with the manifest and whole-artifact
hashes above, then provision them as local CI inputs. Do not auto-capture in CI,
substitute unknown data, or treat absent private fixtures as a passed gate.
No live-provider reliability, throughput, reorg/finality or Stage 13.4/13.5
acceptance is claimed by this offline regression.
