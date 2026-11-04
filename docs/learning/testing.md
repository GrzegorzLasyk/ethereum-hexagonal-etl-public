# Testing the pipeline without confusing evidence levels

Use the [README command table](../../README.md#testing-and-validation-commands)
and existing runners. This guide explains what each level can establish. Do not
run live RPC simply because a local test is missing data.

## Fast tests and architecture

Domain tests cover value constraints, optional fields, identities and observations.
Use-case fakes expose ordering and failure contracts: invalid input must not commit;
ambiguous completion must not trigger a second mutation. Adapter tests use local
HTTP/gRPC peers, controlled responses and contexts to check decoding, cancellation,
retry classification, pacing, exact bytes and raw-to-domain consistency.

Architecture tests parse imports and SQL references, guarding inward dependencies,
no service-internal imports from other services, and owning-schema access. This is
a structural gate, not a substitute for database privileges or transaction tests.

The ordinary root module commands are:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go test -count=1 -v ./test/architecture
```

Tagged integration tests are deliberately absent from these ordinary commands.
The optional live test also requires explicit opt-in and is blocked in normal CI.
Race detection observes exercised interleavings; it does not prove all schedules safe.

## Real PostgreSQL, isolated roles

The three `test/integration/cmd/*-postgres` service runners create unique Compose
projects, credentials and ports, apply migrations, and run owning-adapter tests.
Assertions include cross-schema denial, migration replay/checksum rejection, lease
fencing, admission races, checkpoint rollback and normalized row equivalence.
Failure injection uses disposable test objects; never pass a production database.

`scripts/ci-integration.sh` inspects expected PASS markers and rejects SKIP or missing
tests. Docker failure is a failed/blocked gate, not a successful empty test run.
`ETL_TEST_RETAIN=1` keeps generated resources for diagnosis; otherwise runners remove
only their own disposable project. No global Docker pruning is part of this workflow.

## gRPC handoff and Compose E2E

The handoff runner uses production processes and database roles. Its ACK proxy can
forward an event, let the receiver commit, then lose the response. Durable state
must still converge without duplicate logical records. The full fixture runner adds
consumer outages, database/service restart, replay and a simulated provider reorg.

Success requires all queues and relational records, not only `/ready`, a checkpoint,
a clean process exit or an advanced source cursor. The completion verifier includes
pending/failed/expired-lease/timeout negative cases. Loader batch tests compare every
normalized field (excluding load-time metadata), then force a late batch failure.

## Authentic replay is not new Ethereum traffic

Captured fixtures are private and verified against the versioned manifest before
use. `fixturecapture.NewReplaySource` uses an in-memory RoundTripper that rejects
unknown methods/identities; it has no network fallback. Production RPC decoding is
still exercised. Three noncontiguous heights use three isolated databases rather
than one fictitious contiguous checkpoint.

The independent oracle compares raw bytes, transaction/receipt/log identities,
normalized values and provenance. Phase 1 recorded actual counts and coverage;
absence of authentic type 4 or contract creation is reported, not synthesized.
Ordinary CI does not depend on these private artifacts. Synthetic fixtures are
clearly labeled and are never described as authentic Mainnet data.

## Benchmarks and live validation

`perfbaseline` enables additional probes, but running tagged unit tests alone is
not a performance measurement. The explicit baseline runner needs new output paths,
bounded workloads and all integrity flags before admitting throughput. Keep failed
reports and warmups separate. See [methodology](../architecture/performance-stage14.md).

Final validation separates offline, performance and live modes; none implicitly
runs the other. Live execution needs a private 0600 file, chain/range/attempt/time
bounds and new authorization. After an interrupted session inspect durable state
before deciding whether acquisition is safe to start. Never retry a whole live run
automatically just to obtain better telemetry.

## A reading exercise

In `test/ci/integration_test.go`, observe that the storage flag is explicitly set
per fake-runner case. In Loader `batch_integration_test.go`, observe `to_jsonb(r.*)`:
the table also has a scalar signature column named `r`. In the performance observer,
queue counts and completion timestamps can come from different read snapshots;
both must be complete before latency is reported. These are real test defects found
by validation, retained as lessons rather than weakened assertions.

The [Phase 1](../architecture/validation-offline-stage14.md),
[Phase 2](../architecture/validation-performance-stage14.md) and
[Phase 3](../architecture/validation-live-stage14.md) records distinguish design,
observed results, interrupted attempts and unverified cases. Hosted CI and release
review remain separate evidence for the intended final revision.
