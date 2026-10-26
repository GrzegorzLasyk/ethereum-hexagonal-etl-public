# Protobuf contracts and receiver boundary (Stage 6)

Stage 6 records the transport boundary; later stages implement durable receivers
and dispatch. See the current [operations and release index](operations-stage12.md).

## Contract and ownership

| Handoff | RPC | Payload schema version 1 |
| --- | --- | --- |
| Extractor → Transformer | `etl.v1.TransformerService/SubmitRawBlock` | Exact block JSON result bytes plus separate receipt JSON result bytes in transaction order |
| Transformer → Loader | `etl.v1.LoaderService/SubmitTransformedBlock` | A JSON object, versioned by `metadata.payload_version`; normalized fields are defined in Stage 7 |

Schemas live in `proto/etl/v1/{common,transformer,loader}.proto`, with generated
transport code in `gen/etl/v1/`. Both requests include event and payload schema
versions, canonical decimal chain ID (including zero and values beyond uint64),
present uint64 block height, 32-byte nonzero block hash, 32-byte parent hash,
original extraction time and a non-secret provider label. A zero parent is
allowed only for genesis. Block identity does not assert canonicality/finality.

Stable event IDs are:

- Raw: `raw-block/v1/<chain>/<number>/<hash>`, matching existing Extractor outbox IDs.
- Transformed: `transformed-block/v1/<chain>/<number>/<hash>`.

Chain and number use canonical decimal; hash uses lowercase hex without `0x`.
Never regenerate IDs on retry or derive them from timestamps/providers. The
transformed request also identifies its raw source event and transformation time.
Preserve the first committed event content on retry. Identical duplicates may
be acknowledged only after verifying the existing durable Inbox record; different
content under the same ID is a conflict. Reprocessing with new semantics requires
an explicit version/identity decision, not silently overwriting an event.

Raw JSON is the successful RPC result, not the JSON-RPC response envelope.
Transactions remain in the block; logs remain in each receipt. The receipts
wrapper must be present, including an explicit empty wrapper for an empty block.
Source bytes retain whitespace, numeric spelling and additional fields.

Stage 6 validates envelope fields, supported versions, configured chain,
payload/message sizes and JSON object syntax. It does not normalize quantities,
validate transaction/receipt membership or receipt completeness against block
transactions, reject semantic duplicate JSON keys, or implement normalized
entities. Those checks belong to the service's later application/domain and
parsing boundary in Stage 7/8. A syntactically valid JSON object is not evidence
of valid Ethereum data. No production acceptance use case exists in Stage 6.

## Receiver skeletons and ACK

Each service exposes `NewReceiver(chainID, limits, acceptor)` and
`NewServer(chainID, limits, acceptor)` from its own
`internal/adapters/inbound/grpc` package. The latter registers the generated
service and applies transport limits but opens no listener. Supply an explicit
decimal chain, e.g. `"1"` for the approved Mainnet source.

The acceptor is the service-owned `RawBlockAcceptor` or
`TransformedBlockAcceptor` inbound application port. It uses plain Go strings,
integers, fixed-width hashes, times and borrowed immutable payload buffers.
Passing a nil acceptor creates the Stage 6 skeleton: valid calls return
`Unimplemented`. Stage 7 wires the Transformer executable to a PostgreSQL acceptance use case and
a loopback listener; see [Transformer operation](../../README.md#transformer-operation-stage-7).
Stage 8 likewise wires the Loader to its durable Inbox and loopback listener;
see [Loader operation](../../README.md#loader-operation-stage-8).

An implementation must commit the incoming event to its own Inbox before
returning `DurablyAccepted` or `AlreadyAccepted` with the matching event ID.
Only those explicit outcomes map to a positive `DurableAcceptance`. A nil error
with a zero/unknown result or mismatched event ID returns `Internal`; any error
prevents ACK. Cancellation is checked before delegation and propagated to the
port. If the port reports a durable commit, subsequent cancellation does not
change that fact, although the caller may lose the response on the network.
There is no post-ACK asynchronous in-memory queue.

## Status and retry contract

| Status | Meaning and sender action |
| --- | --- |
| `OK` plus positive acceptance and matching event ID | Inbox durable, possibly a duplicate; not proof of transformation/loading. Stage 9 may mark delivery acknowledged. |
| `InvalidArgument` | Malformed envelope/JSON or application rejection; correct input rather than retrying unchanged. |
| `FailedPrecondition` | Unsupported event/schema version or wrong configured chain; repair configuration/version compatibility. |
| `ResourceExhausted` | Payload/message budget exceeded; retain event and resolve the budget/size problem. No truncation or blind repeated retry. |
| `AlreadyExists` | Same event ID conflicts with previously accepted content; investigate. This is not a duplicate ACK. |
| `Unimplemented` | Inbox acceptance is not implemented; retain event until receiver functionality is deployed. |
| `Unavailable` | Inbox temporarily unavailable; retain event and retry with bounded backoff. |
| `Canceled`, `DeadlineExceeded` | Acceptance/response may be ambiguous; retain and retry the same event ID when appropriate. |
| `Internal` | Unexpected acceptance error or invalid port result; retain event, investigate, and retry the same ID only under the recovery policy. |

Error messages do not expose payloads, provider URLs or underlying database
details. Wrapped application errors retain their classification. The sender
must retain its outbox until a valid positive ACK, including after lost responses.
This is at-least-once delivery; no ordering or exactly-once guarantee is added.
Retry workers, attempt budgets and dispatch are implemented in [Stage 9](handoff-stage9.md). See the
[gRPC status reference](https://grpc.io/docs/guides/status-codes/).

## Byte budgets and memory

`grpccontract.DefaultLimits()` supplies `PayloadBytes: 64 << 20` and
`MessageBytes: 65 << 20`. Constructors accept a `Limits` value for lower
deployment-specific budgets. Validate `0 < payload <= 64 MiB` and
`payload < message <= 65 MiB`. These are explicit adapter settings rather than
implicit gRPC defaults. Record aligned budgets for all peers when wiring them.

The raw budget sums block and receipt bytes, including whitespace, without
joining slices or overflowing a counter. The transformed budget counts its JSON
bytes. Whole-message checks include metadata and protobuf fields.
`NewServer` configures both `grpc.MaxRecvMsgSize` and `grpc.MaxSendMsgSize`;
Stage 9 senders use `limits.ClientOptions()`, which configures both
`grpc.MaxCallSendMsgSize` and `grpc.MaxCallRecvMsgSize`, and explicitly check
payload bytes before sending. Do not override these with incompatible per-call
options. This helper creates no client connection or event transmission.

The gRPC receive budget acts before the handler; the adapter also checks
`proto.Size` for direct calls. Payload limits are checked before JSON scanning.
Reject the entire event on overflow. A source snapshot larger than this budget
can exist under independently configured acquisition limits; it must remain
pending for an explicit resolution, not be silently reduced.

Mapping reuses payload slices and converts only small hash/timestamp fields.
JSON validation scans syntax without decoding to generic maps or reserializing.
`proto.Size` does not serialize a second copy. The port must not mutate buffers
or retain them beyond the call. Future durable persistence must consume them
before ACK; only independently owned copies may outlive the call.

This is not zero-copy gRPC: codecs, network buffers and future database drivers
can allocate additional copies. Many small receipts also create per-element
overhead. Concurrent near-limit calls can consume substantially more memory than
65 MiB each. Future runtime wiring must bound concurrency/in-flight work and
measure memory; the per-message limits alone do not provide that bound.

## Verification and deferred work

`go test ./...` includes metadata validation, both adapter mappings, unchanged
buffer ownership, exact byte boundaries, aggregate raw limits, error/status
mapping, cancellation, delayed/duplicate acceptance responses and rejection of
false ACKs. In-memory gRPC tests exercise registration/serialization, all four
send/receive limits and messages larger than gRPC's usual default receive budget.
They require no TCP listener, provider or PostgreSQL.

Positive acceptance results are simulated only by test-local port fakes. Tests
do not prove durable Inbox storage. Stage 6 adds no migrations, Inbox persistence,
Outbox dispatch, actual service transmission, transformation, normalized storage
or broker. Integration tests for those guarantees belong to their planned stages.

## Generation

Run from the repository root:

```sh
sh scripts/generate-proto.sh
sh scripts/generate-proto.sh --check
```

The script installs exact tool versions into ignored `bin/proto/`, validates
formatting and lint, and generates checked-in Go files under `gen/etl/v1/`.
The check mode generates into a temporary directory and compares the entire
output tree, including missing or extra files. Ordinary Go builds use the
checked-in files and do not need a protobuf compiler or generator downloads.

| Tool | Version |
| --- | --- |
| Buf CLI/compiler | `v1.74.0` |
| `protoc-gen-go` | `v1.36.12` |
| `protoc-gen-go-grpc` | `v1.6.2` |

Buf compiles the descriptors locally, without a separate `protoc` installation
or remote generation plugins. Go module checksums verify tool downloads through
the usual Go module mechanism. Tool installation requires network access on a
cold cache. Runtime gRPC/Protobuf versions are pinned in `go.mod` and `go.sum`;
tool pins live in the script so Buf's build dependencies do not enter the
application module. Use the Go version declared in `go.mod`.

To format edited schemas, after installing tools with the script, run
`bin/proto/buf format -w`. Regenerate and include both schema and generated diffs
in review. Never hand-edit generated files. See the
[Buf generation configuration reference](https://buf.build/docs/configuration/v2/buf-gen-yaml/).

## Evolution

`etl.v1` is the wire API namespace. Event and payload versions are explicit and
independent of generator or software release versions. Version zero is not an
implicit default. A receiver rejects unsupported event/payload versions with
`FailedPrecondition` rather than interpreting them as the current version.

Add optional fields with new field numbers for compatible changes. Preserve
field meanings, presence semantics, RPC names, and enum values. Reserve both
the numbers and names of removed fields; never reuse them. Do not change a
field's type or reinterpret an existing payload version. Changes that invalidate
old readers or alter event identity/acceptance semantics need a reviewed new
version and migration policy. See the
[Protocol Buffers evolution guide](https://protobuf.dev/programming-guides/proto3/#updating).

The configured `FILE` breaking-change policy can compare a proposed change
against a developer-selected, reviewed Git baseline:

```sh
bin/proto/buf breaking --against '.git#ref=<reviewed-contract-commit>'
```

There is no prior protobuf baseline before Stage 6. Compatibility checks do not
prove semantic compatibility of JSON payloads or durable delivery.

Generated types belong to transport adapters. Each receiving service owns its
plain Go acceptance port; neither application nor domain imports generated
code, protobuf, gRPC, or another service's internals. Shared helpers under
`internal/adapters/grpccontract` are transport validation only.
