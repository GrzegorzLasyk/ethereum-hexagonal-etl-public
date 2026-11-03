# Mainnet fixture selection, format v1

`manifest.json` contains **selection metadata only**, not captured RPC data. All
candidates are `capture_pending`. `explorer_verified` means the linked public
explorer metadata supports the claim; it does not mean raw provider responses
have passed the ETL. Null receipt/log counts mean unknown, never zero. Receipt
count 2 for the historical smoke comes from the user's reported live result.

The schema is implemented by Extractor's technical `fixturecapture` adapter.
Unknown fields, unsupported versions, duplicate candidate IDs, missing block
identity, omitted coverage categories and invalid counts fail offline validation.
Every required coverage category must have explorer evidence or an explicit
`pending` explanation. No block/hash/transaction ID may be invented to fill a gap.

Future capture creates an artifact with version 1, the original selection record,
provider label, UTC acquisition time, actual RPC-attempt count, measured counts,
observed feature-to-transaction mappings and SHA-256 hashes (block first, then
receipts in transaction order). Exact raw JSON **result** bytes are base64 fields;
decode base64 to obtain the original bytes without JSON reserialization. Unknown
fields, transaction arrays and all receipt logs remain intact. HTTP headers,
credential-bearing request URLs, JSON-RPC envelope IDs and failed response bodies
are not source snapshots under ADR 0001/0002 and are not saved. This is not a wire
traffic recorder. SHA-256 detects byte changes, not malicious replacement of both
data and metadata, signatures, trie correctness or consensus finality.

Run offline checks from the repository root:

```sh
go run ./services/extractor-service/cmd/fixture-capture manifest test/fixtures/mainnet/v1/manifest.json
# Only once a separately authorized capture exists:
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json historical-smoke .mainnet/fixtures/stage13-3b/1000000.json
```

Neither `manifest`, `validate` nor `verify` constructs a live transport. `verify`
additionally anchors the artifact to the selected manifest entry and checks known
endpoint/key echoes when the private endpoint is supplied in the environment.
Offline validation always rejects Infura endpoint references, even without a key.
`validate` alone checks only the embedded selection's self-consistency; use
`verify` to approve the next capture. Artifact validation
uses the production RPC decoder with an in-memory transport, enforcing chain/
number/hash, full ordered transaction/receipt/log membership, checksums, counts
and the exact transaction witnesses for claimed coverage. Validation failures
are failures, not skipped coverage. The test suite currently validates synthetic
artifacts and the selection manifest; no real raw fixture exists in this version.

Capture outputs belong in ignored `.mainnet/fixtures/` initially. They must be
reviewed for completeness, credentials, size and reproducibility before a future
separately requested promotion into versioned test data. Do not edit source JSON
to make a decoder test pass. See the [Stage 13.3A runbook](../../../../docs/architecture/mainnet-fixtures-stage13.md)
for bounds, prerequisites, selection evidence and the planned full ETL assertions.
